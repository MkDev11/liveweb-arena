## Problem
Owner request:

> Please add more question templates to OpenLibrary. The newly added problem templates must be suitable for RL training - specifically, SFT should not be able to achieve high scores simply by learning fixed patterns.

The Open Library plugin previously lacked templates for cross-author comparison, engagement extrema, and threshold-based engagement filtering.

## Solution
Add 3 new OpenLibrary templates using only confirmed-visible, dynamic engagement metrics (`ratings_count`, `want_to_read_count`, `already_read_count`) and require multi-step reasoning over live search results.

### New templates

| ID | Template | Difficulty | What it tests | Variant space |
|----|----------|-----------|---------------|---------------|
| 96 | `openlibrary_author_engagement_extrema` | Medium | Find book with highest/lowest engagement metric among author top N | 1,680 |
| 97 | `openlibrary_author_comparison` | Medium/Hard | Compare aggregate engagement between two authors (requires 2 searches) | 14,490 |
| 98 | `openlibrary_reading_stats_filter` | Hard | Count books meeting engagement threshold (no single-sort shortcut) | 1,680 |

### Why this is RL-suitable (not fixed-pattern SFT)
- Dynamic answers: engagement counts change over time
- Computation required: extrema, aggregation, threshold counting
- Large combinatorial space: all templates exceed 500 variants
- No static shortcut: exact answers require browsing and page-grounded calculation

### Refactoring / Duplication reduction
Moved shared author-query matching helpers (`normalize_author_fragment`, `extract_author_filter`, `find_author_search_entry`) into `templates/common.py` and reused them across author templates.

### Live-eval robustness improvement
Added a controlled fallback in `find_author_search_entry`:
- Prefer exact author + exact sort match (`sort=editions`) when available
- If missing, optionally fall back to unsorted author-matched search data

This preserves strict behavior by default and only enables fallback where needed (`96/97/98`) to reduce fragile `not_collected` outcomes in live runs.

## Testing
- `tests/plugins/openlibrary/test_engagement_templates.py`: **65 passed**
- `tests/plugins/openlibrary`: **106 passed**
- full repository suite: **360 passed**

Added tests cover:
- generation invariants and template registration
- GT correctness, tie-breaking, missing data, no-match cases
- helper behavior (sort strictness, plain-text author matching, unsorted fallback)
- regression checks for live-like unsorted query collection

## Edge Cases Handled
- Tied metric values -> alphabetically earlier title/author wins
- Missing metric field -> explicit GT failure
- Exact threshold boundary -> strict `>` (equality not counted)
- Zero matches -> `"0"` valid
- Insufficient collected works -> explicit failure with required/actual count
- Invalid validation inputs -> type-checked failure
- Unsorted author search collection in live runs -> fallback supported for new templates
