# Agent instructions for SquishDB

Follow these rules on every change, commit, pull request, and merge request in this repository.

## Merge request checklist (mandatory)

Every merge request or pull request must update the README benchmark charts before it is opened or merged.

1. Build in release mode with `cargo build --release`.
2. Run the full test suite in debug and release mode: `cargo test --all-targets` and `cargo test --release --all-targets`.
3. Run the benchmark suite when performance, compression, storage layout, indexing, query execution, or concurrency changed:
   - `python3 benchmarks/compare_sqlite.py balanced`
   - `python3 benchmarks/compare_industry.py`
   - `python3 benchmarks/bench_v2.py`
4. Regenerate [docs/charts/storage-comparison.svg](docs/charts/storage-comparison.svg) and [docs/charts/speed-comparison.svg](docs/charts/speed-comparison.svg) via `python3 benchmarks/update_charts.py`, and update the benchmark table and throughput numbers in [README.md](README.md) so the prose and SVG charts match the latest measured numbers.
5. Never submit stale charts or hand-edited numbers that do not match the JSON benchmark output.

## Engineering rules

- Make the smallest change that fully solves the task. Skip options, wrappers, and boilerplate nobody asked for.
- Reuse existing modules and patterns in the codebase. Keep pure-Rust dependencies minimal; do not introduce C toolchains or heavy crates when `std` solves the job.
- Finish every caller, test, binding, and doc your change affects.
- Every non-trivial feature or bug fix needs focused tests covering edge cases and crash or concurrency behavior where relevant.
- Never cut validation at trust boundaries, checksums, crash safety, or error handling.
- If a shortcut has a known limit, document it in one comment: `shortcut: <limit>, <when to upgrade>`.

## Writing rules (unslop)

Apply these rules to all docs, README edits, comments, CLI text, commit messages, PR descriptions, and replies.

### Voice and substance
- State concrete facts, mechanisms, and measured numbers. Cut sentences that describe how something "feels" or that could be pasted unchanged into another project's README.
- Vary sentence length. Keep one idea per sentence and split dense clauses.
- Use active voice. Name the actor instead of writing "is validated" or "are processed".
- Cut weak adverbs ("significantly", "quickly", "seamlessly"). Use a stronger verb or the measured delta.
- Use plain words: "use" instead of "utilize" or "leverage", "help" instead of "facilitate", "many" instead of "numerous", "if" instead of "in the event that".

### Patterns to remove
1. **No puffery or promotional language.** Cut "pivotal", "testament", "evolving landscape", "groundbreaking", "stunning", "vibrant", "robust", "cutting-edge", "blazing-fast", "world-class". State what the code does.
2. **No AI vocabulary.** Avoid "additionally", "crucial", "delve", "enduring", "enhance", "fostering", "garner", "interplay", "intricate", "landscape" (abstract), "showcase", "tapestry", "underscore".
3. **No superficial `-ing` tails.** Cut trailing clauses like ", ensuring reliability", ", highlighting performance", ", reflecting best practices".
4. **No fancy substitutes for "is" or "has".** Replace "serves as", "stands as", "boasts", "features" with "is" or "has".
5. **No "not just X, but Y" framing.** State the point directly.
6. **No forced rule of three or synonym cycling.** Use the natural number of items, and reuse the same term instead of cycling synonyms.
7. **No false ranges.** Do not write "from X to Y" unless X and Y sit on a real scale.
8. **No em dashes.** Never use em dashes, en dashes, or hyphen-as-dash substitutes in prose, and do not swap them for parenthetical crutches. End the sentence with a period or use a comma.
9. **No mid-sentence colons.** Colons belong before a list or code example, not as connectors between clauses.
10. **No boldface spam or inline-header restatements.** Do not bold every acronym or proper noun. Convert "**Performance:** Performance improved..." into plain prose. Bold lead-ins that end in a period and introduce new detail are fine.
11. **Sentence case headings.** Write `## How it works`, not `## How It Works`.
12. **No decorative emojis and no curly quotes.** Use plain ASCII straight quotes (`'` and `"`).
13. **No abstract metaphor nouns.** Avoid "substrate", "wedge", "vector" (as metaphor), "locus", "vantage", "nexus", "primitive" (as noun), "harness" (as metaphor), "API surface", "bedrock", "scaffolding", "modality", "paradigm", "gold-plating", "ratchet", "endgame", "north star", "flywheel". Name the actual component.
14. **No filler or hedging.** Turn "in order to" into "to", "due to the fact that" into "because", and delete "it is important to note that". Replace stacked hedges with "may" or state the condition directly.
15. **No chatbot filler.** Cut "I hope this helps!", "Certainly!", "Great question!", and generic conclusions like "The future looks bright."
