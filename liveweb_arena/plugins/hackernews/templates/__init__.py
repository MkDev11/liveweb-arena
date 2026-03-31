"""Hacker News question templates.

RL-friendly template design:
- All templates require multi-step reasoning
- All templates require computation or comparison
- All templates have large exploration space
- Low memorization risk due to dynamic data and combinatorial question space
"""

from .multi_condition_filter import HackerNewsMultiConditionFilterTemplate
from .extrema_comparison import HackerNewsExtremaComparisonTemplate
from .category_comparison import HackerNewsCategoryComparisonTemplate
from .news_summary import HackerNewsNewsSummaryTemplate
from .derived_metric import HackerNewsDerivedMetricTemplate
from .weighted_rank import HackerNewsWeightedRankTemplate

__all__ = [
    "HackerNewsMultiConditionFilterTemplate",
    "HackerNewsExtremaComparisonTemplate",
    "HackerNewsCategoryComparisonTemplate",
    "HackerNewsNewsSummaryTemplate",
    "HackerNewsDerivedMetricTemplate",
    "HackerNewsWeightedRankTemplate",
]
