use std::any::Any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct PoolState {
    queue: Vec<Job>,
    shutdown: bool,
}

/// RAII barrier ensuring all dispatched worker jobs finish before `execute_indexed` can return
/// OR unwind due to a panic on the calling thread, preventing dangling pointer dereferences.
struct WaitGuard {
    remaining: Arc<(Mutex<usize>, Condvar)>,
}

impl WaitGuard {
    fn wait_now(&self) {
        let (lock, cvar) = &*self.remaining;
        let mut r = lock.lock().unwrap_or_else(|e| e.into_inner());
        while *r > 0 {
            r = cvar.wait(r).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.wait_now();
    }
}

/// Reusable scoped thread pool for parallel page scans, vectorized GROUP BY,
/// parallel PAX page compression, and parallel index builds.
pub struct ParallelPool {
    state: Arc<(Mutex<PoolState>, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    num_threads: usize,
}

impl ParallelPool {
    pub fn new(requested: usize) -> Self {
        let num_threads = if requested == 0 {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
                .clamp(1, 8)
        } else {
            requested.clamp(1, 32)
        };

        let state = Arc::new((
            Mutex::new(PoolState {
                queue: Vec::new(),
                shutdown: false,
            }),
            Condvar::new(),
        ));

        let mut workers = Vec::with_capacity(num_threads.saturating_sub(1));
        if num_threads > 1 {
            // Leader thread participates too, so spawn num_threads - 1 background workers.
            for idx in 0..(num_threads - 1) {
                let st = Arc::clone(&state);
                let handle = thread::Builder::new()
                    .name(format!("squishdb-worker-{}", idx))
                    .spawn(move || worker_loop(st))
                    .expect("failed to spawn SquishDB worker thread");
                workers.push(handle);
            }
        }

        ParallelPool {
            state,
            workers,
            num_threads,
        }
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.num_threads
    }

    /// Executes `f` across `items` in parallel (with the calling thread executing shard 0)
    /// and returns results in input order. Propagates any panic from either the calling thread
    /// or a worker thread only after all dispatched workers have finished.
    pub fn execute_indexed<T, R, F>(&self, items: &[T], f: F) -> Vec<R>
    where
        T: Sync,
        R: Send,
        F: Fn(usize, &T) -> R + Sync,
    {
        if items.is_empty() {
            return Vec::new();
        }
        if self.num_threads <= 1 || items.len() <= 1 {
            return items
                .iter()
                .enumerate()
                .map(|(i, item)| f(i, item))
                .collect();
        }

        let n = items.len();
        let workers = self.num_threads.min(n);
        let chunk_size = (n + workers - 1) / workers;
        let num_chunks = (n + chunk_size - 1) / chunk_size;

        let mut slots: Vec<Mutex<Option<Vec<R>>>> =
            (0..num_chunks).map(|_| Mutex::new(None)).collect();
        let remaining = Arc::new((Mutex::new(num_chunks.saturating_sub(1)), Condvar::new()));
        let worker_panic: Arc<Mutex<Option<Box<dyn Any + Send>>>> = Arc::new(Mutex::new(None));

        let f_ref: &F = &f;
        let items_ptr = items.as_ptr() as usize;
        let items_len = items.len();
        let f_ptr = f_ref as *const F as usize;
        let slots_ptr = slots.as_ptr() as usize;

        // Guard declared AFTER `slots` so Drop runs BEFORE `slots` is dropped (LIFO drop order).
        let guard = WaitGuard {
            remaining: Arc::clone(&remaining),
        };

        if num_chunks > 1 {
            let (lock, cvar) = &*self.state;
            let mut st = lock.lock().unwrap();
            for chunk_idx in 1..num_chunks {
                let start = chunk_idx * chunk_size;
                let end = (start + chunk_size).min(n);
                let rem = Arc::clone(&remaining);
                let panic_slot = Arc::clone(&worker_panic);
                // SAFETY: `guard` drops before `slots` and before `execute_indexed` returns or
                // unwinds, blocking until every dispatched worker has completed and released
                // all references to `items`, `f`, and `slots`.
                let job: Job = Box::new(move || {
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        let slice =
                            unsafe { std::slice::from_raw_parts(items_ptr as *const T, items_len) };
                        let func = unsafe { &*(f_ptr as *const F) };
                        let mut local = Vec::with_capacity(end - start);
                        for i in start..end {
                            local.push(func(i, &slice[i]));
                        }
                        let slot =
                            unsafe { &*(slots_ptr as *const Mutex<Option<Vec<R>>>).add(chunk_idx) };
                        *slot.lock().unwrap() = Some(local);
                    }));
                    if let Err(payload) = result {
                        let mut p = panic_slot.lock().unwrap_or_else(|e| e.into_inner());
                        if p.is_none() {
                            *p = Some(payload);
                        }
                    }
                    let (rlock, rcvar) = &*rem;
                    let mut r = rlock.lock().unwrap_or_else(|e| e.into_inner());
                    *r -= 1;
                    if *r == 0 {
                        rcvar.notify_all();
                    }
                });
                st.queue.push(job);
            }
            cvar.notify_all();
        }

        // Leader executes chunk 0; catch any panic so we wait for workers before re-raising.
        let end0 = chunk_size.min(n);
        let leader_res = catch_unwind(AssertUnwindSafe(|| {
            let mut local0 = Vec::with_capacity(end0);
            for i in 0..end0 {
                local0.push(f(i, &items[i]));
            }
            *slots[0].lock().unwrap() = Some(local0);
        }));

        // Wait for all workers to finish before inspecting panics or touching `slots`.
        guard.wait_now();
        drop(guard);

        if let Err(payload) = leader_res {
            resume_unwind(payload);
        }
        if let Some(payload) = worker_panic
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            resume_unwind(payload);
        }

        let mut out = Vec::with_capacity(n);
        for slot in &mut slots {
            if let Some(chunk_out) = slot.get_mut().unwrap().take() {
                out.extend(chunk_out);
            }
        }
        out
    }

    /// Runs `num_shards` independent shard closures in parallel, returning each shard's output.
    pub fn execute_shards<R, F>(&self, num_shards: usize, f: F) -> Vec<R>
    where
        R: Send,
        F: Fn(usize, usize) -> R + Sync,
    {
        if num_shards == 0 {
            return Vec::new();
        }
        if self.num_threads <= 1 || num_shards == 1 {
            return (0..num_shards).map(|s| f(s, num_shards)).collect();
        }
        let ids: Vec<usize> = (0..num_shards).collect();
        self.execute_indexed(&ids, |_, &shard_id| f(shard_id, num_shards))
    }
}

fn worker_loop(state: Arc<(Mutex<PoolState>, Condvar)>) {
    let (lock, cvar) = &*state;
    loop {
        let job = {
            let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
            while st.queue.is_empty() && !st.shutdown {
                st = cvar.wait(st).unwrap_or_else(|e| e.into_inner());
            }
            if st.shutdown && st.queue.is_empty() {
                return;
            }
            st.queue.pop()
        };
        if let Some(j) = job {
            j();
        }
    }
}

impl Drop for ParallelPool {
    fn drop(&mut self) {
        {
            let (lock, cvar) = &*self.state;
            let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
            st.shutdown = true;
            cvar.notify_all();
        }
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}
