"""Open Library question templates.

RL-friendly template design:
- All templates require search or navigation to find data
- Dynamic data prevents memorization (edition counts, ratings change)
- Large entity pool (millions of works across 50+ subjects)
"""

from .book_stats import OpenLibraryBookStatsTemplate
from .subject_multi_condition import OpenLibrarySubjectMultiConditionTemplate

__all__ = [
    "OpenLibraryBookStatsTemplate",
    "OpenLibrarySubjectMultiConditionTemplate",
]
