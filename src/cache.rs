use std::collections::{HashMap, VecDeque};

use crate::types::Row;
use crate::vector::ColumnVector;

/// Bounded in-memory LRU cache for decoded PAX pages (row form) AND individual decoded
/// typed `ColumnVector`s keyed by `(page_id, col_key)` (`col_key = u32::MAX` for rowids).
pub struct PageCache {
    max_pages: usize,
    entries: HashMap<u64, Vec<Row>>,
    order: VecDeque<u64>,
    max_cols: usize,
    col_entries: HashMap<(u64, u32), ColumnVector>,
    col_order: VecDeque<(u64, u32)>,
    pub hits: u64,
    pub misses: u64,
}

impl PageCache {
    pub fn new(max_pages: usize) -> Self {
        PageCache {
            max_pages: max_pages.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_cols: (max_pages * 8).max(32),
            col_entries: HashMap::new(),
            col_order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, page_id: u64) -> Option<&[Row]> {
        if self.entries.contains_key(&page_id) {
            self.hits += 1;
            if let Some(pos) = self.order.iter().position(|&id| id == page_id) {
                self.order.remove(pos);
            }
            self.order.push_back(page_id);
            return self.entries.get(&page_id).map(|v| v.as_slice());
        }
        self.misses += 1;
        None
    }

    pub fn insert(&mut self, page_id: u64, rows: Vec<Row>) {
        if self.entries.contains_key(&page_id) {
            if let Some(pos) = self.order.iter().position(|&id| id == page_id) {
                self.order.remove(pos);
            }
        } else if self.entries.len() >= self.max_pages {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
        self.entries.insert(page_id, rows);
        self.order.push_back(page_id);
    }

    #[inline]
    pub fn get_col(&mut self, page_id: u64, col_key: u32) -> Option<&ColumnVector> {
        let key = (page_id, col_key);
        if self.col_entries.contains_key(&key) {
            self.hits += 1;
            return self.col_entries.get(&key);
        }
        self.misses += 1;
        None
    }

    pub fn insert_col(&mut self, page_id: u64, col_key: u32, vec: ColumnVector) {
        let key = (page_id, col_key);
        if !self.col_entries.contains_key(&key) {
            if self.col_entries.len() >= self.max_cols {
                if let Some(evicted) = self.col_order.pop_front() {
                    self.col_entries.remove(&evicted);
                }
            }
            self.col_order.push_back(key);
        }
        self.col_entries.insert(key, vec);
    }

    pub fn invalidate(&mut self, page_id: u64) {
        if self.entries.remove(&page_id).is_some() {
            if let Some(pos) = self.order.iter().position(|&id| id == page_id) {
                self.order.remove(pos);
            }
        }
        self.col_entries.retain(|&(pid, _), _| pid != page_id);
        self.col_order.retain(|&(pid, _)| pid != page_id);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.col_entries.clear();
        self.col_order.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
