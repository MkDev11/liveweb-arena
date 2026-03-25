"""Tests for Open Library engagement & comparison templates.

Covers:
1. Template registration and generation invariants
2. author_engagement_extrema GT behavior and edge cases
3. author_comparison GT behavior and edge cases
4. reading_stats_filter GT behavior and edge cases
5. Task registry wiring (IDs 96, 97, 98)
6. Shared helper refactoring (common.py)
7. Cross-template consistency (serialization, GT source, cache source)
"""

import asyncio
from typing import Any, Dict, List, Optional

import pytest

from liveweb_arena.core.gt_collector import GTSourceType, set_current_gt_collector
from liveweb_arena.core.task_registry import TaskRegistry
from liveweb_arena.core.validators.base import get_registered_templates
from liveweb_arena.plugins.openlibrary.templates.author_comparison import (
    AuthorMetric,
    OpenLibraryAuthorComparisonTemplate,
)
from liveweb_arena.plugins.openlibrary.templates.author_engagement_extrema import (
    EngagementMetric,
    OpenLibraryAuthorEngagementExtremaTemplate,
)
from liveweb_arena.plugins.openlibrary.templates.author_editions import AUTHOR_POOL
from liveweb_arena.plugins.openlibrary.templates.common import (
    extract_author_filter,
    find_author_search_entry,
    normalize_author_fragment,
)
from liveweb_arena.plugins.openlibrary.templates.reading_stats_filter import (
    OpenLibraryReadingStatsFilterTemplate,
    ReaderMetric,
)


class _DummyCollector:
    def __init__(self, data: Dict[str, Dict[str, Any]]):
        self._data = data

    def get_collected_api_data(self) -> Dict[str, Dict[str, Any]]:
        return self._data


def _run_gt(data: Dict[str, Dict[str, Any]], coro):
    set_current_gt_collector(_DummyCollector(data))
    try:
        return asyncio.run(coro)
    finally:
        set_current_gt_collector(None)


def _make_search_entry(
    query: str, sort: Optional[str], works: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "query": query,
        "sort": sort,
        "works": {work["key"]: work for work in works},
    }


# ── 1. Template registration ──────────────────────────────────────────

SEEDS = [1, 42, 100, 999, 12345]


@pytest.mark.parametrize("name", [
    "openlibrary_author_engagement_extrema",
    "openlibrary_author_comparison",
    "openlibrary_reading_stats_filter",
])
def test_template_registered(name):
    templates = get_registered_templates()
    assert name in templates, f"template '{name}' not registered"


# ── 2. Generation invariants ──────────────────────────────────────────


@pytest.mark.parametrize("seed", SEEDS)
def test_engagement_extrema_generate(seed):
    q = OpenLibraryAuthorEngagementExtremaTemplate().generate(seed)
    assert q.question_text
    assert "openlibrary.org" in q.start_url
    assert q.template_name == "openlibrary_author_engagement_extrema"
    assert q.validation_info["extrema"] in {"highest", "lowest"}
    assert q.validation_info["metric"] in {
        "want_to_read_count", "ratings_count",
    }
    assert q.validation_info["work_count"] in {3, 5, 7, 10}
    assert "q=author%3A%22" in q.start_url
    assert "sort=editions" in q.start_url


@pytest.mark.parametrize("seed", SEEDS)
def test_author_comparison_generate(seed):
    q = OpenLibraryAuthorComparisonTemplate().generate(seed)
    assert q.question_text
    assert "openlibrary.org" in q.start_url
    assert q.template_name == "openlibrary_author_comparison"
    assert q.validation_info["author_a_name"] != q.validation_info["author_b_name"]
    assert q.validation_info["metric"] in {
        "ratings_count", "want_to_read_count",
    }
    assert q.validation_info["work_count"] in {3, 5}


@pytest.mark.parametrize("seed", SEEDS)
def test_reading_stats_filter_generate(seed):
    q = OpenLibraryReadingStatsFilterTemplate().generate(seed)
    assert q.question_text
    assert "openlibrary.org" in q.start_url
    assert q.template_name == "openlibrary_reading_stats_filter"
    assert q.validation_info["metric"] in {
        "want_to_read_count", "ratings_count",
    }
    assert q.validation_info["work_count"] in {5, 10}
    assert isinstance(q.validation_info["threshold"], int)


def test_author_comparison_distinct_authors_all_seeds():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    for seed in range(1, 30):
        q = tmpl.generate(seed)
        assert q.validation_info["author_a_name"] != q.validation_info["author_b_name"], (
            f"seed={seed}: same author selected twice"
        )


def test_author_comparison_position_swap_occurs():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    pairs = set()
    for seed in range(1, 50):
        q = tmpl.generate(seed)
        pairs.add((q.validation_info["author_a_name"], q.validation_info["author_b_name"]))
    assert len(pairs) > 10, "Position bias: too few unique ordered pairs"


# ── 3. author_engagement_extrema GT behavior ──────────────────────────


def test_extrema_finds_highest_want_to_read():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "want_to_read_count": 10000},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "want_to_read_count": 2000},
            {"key": "/works/OL3W", "rank": 3, "title": "Misery", "want_to_read_count": 2500},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Stephen King", "author_query": "stephen king",
        "search_query": 'author:"stephen king"', "sort": "editions",
        "work_count": 3, "extrema": "highest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is True
    assert result.value == "It"


def test_extrema_finds_lowest_want_to_read():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:austen": _make_search_entry('author:"jane austen"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "Sense and Sensibility", "want_to_read_count": 50},
            {"key": "/works/OL2W", "rank": 2, "title": "Pride and Prejudice", "want_to_read_count": 500},
            {"key": "/works/OL3W", "rank": 3, "title": "Emma", "want_to_read_count": 200},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Jane Austen", "author_query": "jane austen",
        "search_query": 'author:"jane austen"', "sort": "editions",
        "work_count": 3, "extrema": "lowest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is True
    assert result.value == "Sense and Sensibility"


def test_extrema_matches_unsorted_query_when_sort_not_collected():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:austen": _make_search_entry("jane austen", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "Sense and Sensibility", "want_to_read_count": 50},
            {"key": "/works/OL2W", "rank": 2, "title": "Pride and Prejudice", "want_to_read_count": 500},
            {"key": "/works/OL3W", "rank": 3, "title": "Emma", "want_to_read_count": 200},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Jane Austen", "author_query": "jane austen",
        "search_query": 'author:"jane austen"', "sort": "editions",
        "work_count": 3, "extrema": "lowest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is True
    assert result.value == "Sense and Sensibility"


def test_extrema_tie_breaks_alphabetically():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:dickens": _make_search_entry('author:"charles dickens"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "Oliver Twist", "ratings_count": 100},
            {"key": "/works/OL2W", "rank": 2, "title": "David Copperfield", "ratings_count": 100},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Charles Dickens", "author_query": "charles dickens",
        "search_query": 'author:"charles dickens"', "sort": "editions",
        "work_count": 2, "extrema": "highest", "metric": "ratings_count",
        "metric_label": "number of ratings",
    }))
    assert result.success is True
    assert result.value == "David Copperfield"  # alphabetically earlier


def test_extrema_not_collected_wrong_author():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:dickens": _make_search_entry('author:"charles dickens"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "X", "want_to_read_count": 100},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Jane Austen", "author_query": "jane austen",
        "search_query": 'author:"jane austen"', "sort": "editions",
        "work_count": 3, "extrema": "highest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is False
    assert result.is_data_not_collected()


def test_extrema_missing_metric_treated_as_zero():
    """OL API omits count fields when the value is zero; GT treats absent as 0."""
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    collected = {
        "ol:search:dickens": _make_search_entry('author:"charles dickens"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "Oliver Twist", "want_to_read_count": 100},
            {"key": "/works/OL2W", "rank": 2, "title": "David Copperfield"},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Charles Dickens", "author_query": "charles dickens",
        "search_query": 'author:"charles dickens"', "sort": "editions",
        "work_count": 2, "extrema": "highest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is True
    assert result.value == "Oliver Twist"  # 100 > 0 (missing treated as 0)


def test_extrema_no_collected_data():
    tmpl = OpenLibraryAuthorEngagementExtremaTemplate()
    result = _run_gt({}, tmpl.get_ground_truth({
        "author_name": "X", "author_query": "x",
        "search_query": 'author:"x"', "sort": "editions",
        "work_count": 3, "extrema": "highest", "metric": "want_to_read_count",
        "metric_label": "want-to-read count",
    }))
    assert result.success is False


# ── 4. author_comparison GT behavior ──────────────────────────────────


def test_comparison_picks_higher_total():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 500},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "ratings_count": 200},
        ]),
        "ol:search:christie": _make_search_entry('author:"agatha christie"', "editions", [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles", "ratings_count": 100},
            {"key": "/works/OL4W", "rank": 2, "title": "Adversary", "ratings_count": 50},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 2, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is True
    assert result.value == "Stephen King"  # 700 > 150


def test_comparison_reverse_winner():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "want_to_read_count": 100},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "want_to_read_count": 50},
        ]),
        "ol:search:christie": _make_search_entry('author:"agatha christie"', "editions", [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles", "want_to_read_count": 800},
            {"key": "/works/OL4W", "rank": 2, "title": "Adversary", "want_to_read_count": 300},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 2, "metric": "want_to_read_count",
        "metric_label": "total want-to-read count",
    }))
    assert result.success is True
    assert result.value == "Agatha Christie"  # 1100 > 150


def test_comparison_tie_breaks_alphabetically():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 300},
        ]),
        "ol:search:christie": _make_search_entry('author:"agatha christie"', "editions", [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles", "ratings_count": 300},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 1, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is True
    assert result.value == "Agatha Christie"  # alphabetically earlier


def test_comparison_matches_unsorted_queries_when_sort_not_collected():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry("stephen king", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 500},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "ratings_count": 200},
        ]),
        "ol:search:christie": _make_search_entry("agatha christie", None, [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles", "ratings_count": 100},
            {"key": "/works/OL4W", "rank": 2, "title": "Adversary", "ratings_count": 50},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 2, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is True
    assert result.value == "Stephen King"


def test_comparison_not_collected_missing_author():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 500},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 1, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is False
    assert result.is_data_not_collected()


def test_comparison_no_collected_data():
    tmpl = OpenLibraryAuthorComparisonTemplate()
    result = _run_gt({}, tmpl.get_ground_truth({
        "author_a_name": "A", "author_a_query": "a",
        "search_query_a": 'author:"a"',
        "author_b_name": "B", "author_b_query": "b",
        "search_query_b": 'author:"b"',
        "sort": "editions", "work_count": 1, "metric": "ratings_count",
        "metric_label": "x",
    }))
    assert result.success is False


def test_comparison_missing_metric_treated_as_zero():
    """OL API omits count fields when the value is zero; GT treats absent as 0."""
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 500},
        ]),
        "ol:search:christie": _make_search_entry('author:"agatha christie"', "editions", [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles"},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 1, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is True
    assert result.value == "Stephen King"  # 500 > 0 (missing treated as 0)


# ── 5. reading_stats_filter GT behavior ───────────────────────────────


def test_filter_counts_above_threshold():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "want_to_read_count": 10000},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "want_to_read_count": 2000},
            {"key": "/works/OL3W", "rank": 3, "title": "Misery", "want_to_read_count": 2500},
            {"key": "/works/OL4W", "rank": 4, "title": "The Shining", "want_to_read_count": 150},
            {"key": "/works/OL5W", "rank": 5, "title": "Salem's Lot", "want_to_read_count": 50},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Stephen King", "author_query": "stephen king",
        "search_query": 'author:"stephen king"', "sort": "editions",
        "work_count": 5, "metric": "want_to_read_count",
        "metric_label": "people who want to read them", "threshold": 200,
    }))
    assert result.success is True
    assert result.value == "3"  # It(10000), Carrie(2000), Misery(2500) > 200


def test_filter_returns_zero_when_none_match():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:poe": _make_search_entry('author:"edgar allan poe"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "The Raven", "ratings_count": 10},
            {"key": "/works/OL2W", "rank": 2, "title": "Annabel Lee", "ratings_count": 5},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Edgar Allan Poe", "author_query": "edgar allan poe",
        "search_query": 'author:"edgar allan poe"', "sort": "editions",
        "work_count": 2, "metric": "ratings_count",
        "metric_label": "ratings", "threshold": 500,
    }))
    assert result.success is True
    assert result.value == "0"


def test_filter_exact_threshold_not_counted():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:poe": _make_search_entry('author:"edgar allan poe"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "The Raven", "ratings_count": 100},
            {"key": "/works/OL2W", "rank": 2, "title": "Annabel Lee", "ratings_count": 101},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Edgar Allan Poe", "author_query": "edgar allan poe",
        "search_query": 'author:"edgar allan poe"', "sort": "editions",
        "work_count": 2, "metric": "ratings_count",
        "metric_label": "ratings", "threshold": 100,
    }))
    assert result.success is True
    assert result.value == "1"  # only 101 > 100, not 100 > 100


def test_filter_matches_unsorted_query_when_sort_not_collected():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:king": _make_search_entry("stephen king", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "want_to_read_count": 10000},
            {"key": "/works/OL2W", "rank": 2, "title": "Carrie", "want_to_read_count": 2000},
            {"key": "/works/OL3W", "rank": 3, "title": "Misery", "want_to_read_count": 2500},
            {"key": "/works/OL4W", "rank": 4, "title": "The Shining", "want_to_read_count": 150},
            {"key": "/works/OL5W", "rank": 5, "title": "Salem's Lot", "want_to_read_count": 50},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Stephen King", "author_query": "stephen king",
        "search_query": 'author:"stephen king"', "sort": "editions",
        "work_count": 5, "metric": "want_to_read_count",
        "metric_label": "people who want to read them", "threshold": 200,
    }))
    assert result.success is True
    assert result.value == "3"


def test_filter_not_collected_wrong_author():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:poe": _make_search_entry('author:"edgar allan poe"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "X", "want_to_read_count": 100},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Mark Twain", "author_query": "mark twain",
        "search_query": 'author:"mark twain"', "sort": "editions",
        "work_count": 5, "metric": "want_to_read_count",
        "metric_label": "people who want to read them", "threshold": 100,
    }))
    assert result.success is False
    assert result.is_data_not_collected()


def test_filter_missing_metric_treated_as_zero():
    """OL API omits count fields when the value is zero; GT treats absent as 0."""
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    collected = {
        "ol:search:poe": _make_search_entry('author:"edgar allan poe"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "The Raven", "want_to_read_count": 100},
            {"key": "/works/OL2W", "rank": 2, "title": "Annabel Lee"},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_name": "Edgar Allan Poe", "author_query": "edgar allan poe",
        "search_query": 'author:"edgar allan poe"', "sort": "editions",
        "work_count": 2, "metric": "want_to_read_count",
        "metric_label": "people who want to read them", "threshold": 50,
    }))
    assert result.success is True
    assert result.value == "1"  # only The Raven (100) > 50; Annabel Lee (0) is not


def test_filter_no_collected_data():
    tmpl = OpenLibraryReadingStatsFilterTemplate()
    result = _run_gt({}, tmpl.get_ground_truth({
        "author_name": "X", "author_query": "x",
        "search_query": 'author:"x"', "sort": "editions",
        "work_count": 5, "metric": "want_to_read_count",
        "metric_label": "people who want to read them", "threshold": 100,
    }))
    assert result.success is False


# ── 6. Task registry ──────────────────────────────────────────────────


def test_task_registry_new_template_ids():
    assert TaskRegistry.TEMPLATES[96] == (
        "openlibrary", "openlibrary_author_engagement_extrema",
    )
    assert TaskRegistry.TEMPLATES[97] == (
        "openlibrary", "openlibrary_author_comparison",
    )
    assert TaskRegistry.TEMPLATES[98] == (
        "openlibrary", "openlibrary_reading_stats_filter",
    )


def test_task_registry_version_7_entry():
    found = any(sorted(v) == [96, 97, 98] for v in TaskRegistry.TEMPLATE_VERSIONS)
    assert found, "No TEMPLATE_VERSIONS entry for [96, 97, 98]"


# ── 7. Shared helper refactoring ──────────────────────────────────────


def test_normalize_author_fragment():
    assert normalize_author_fragment("Mark Twain") == "mark twain"
    assert normalize_author_fragment("H.G. Wells") == "h g wells"
    assert normalize_author_fragment("J.K. Rowling") == "j k rowling"
    assert normalize_author_fragment("") == ""


def test_extract_author_filter_standard():
    assert extract_author_filter('author:"mark twain"') == "mark twain"
    assert extract_author_filter("AUTHOR: \"Mark Twain\"") == "mark twain"
    assert extract_author_filter("author:'h.g. wells'") == "h g wells"


def test_extract_author_filter_rejects_plain_text():
    assert extract_author_filter("mark twain") is None
    assert extract_author_filter("") is None


def test_find_author_search_entry_matches():
    collected = {
        "ol:search:twain": _make_search_entry('author:"mark twain"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "X"},
        ]),
    }
    result = find_author_search_entry(
        collected, search_query='author:"mark twain"', sort="editions",
    )
    assert result is not None
    assert result["query"] == 'author:"mark twain"'


def test_find_author_search_entry_rejects_wrong_sort():
    collected = {
        "ol:search:twain": _make_search_entry('author:"mark twain"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "X"},
        ]),
    }
    result = find_author_search_entry(
        collected, search_query='author:"mark twain"', sort="new",
    )
    assert result is None


def test_find_author_search_entry_unsorted_fallback_disabled_by_default():
    collected = {
        "ol:search:christie": _make_search_entry("agatha christie", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "Styles"},
        ]),
    }
    result = find_author_search_entry(
        collected, search_query='author:"agatha christie"', sort="editions",
    )
    assert result is None


def test_find_author_search_entry_matches_unsorted_when_fallback_enabled():
    collected = {
        "ol:search:christie": _make_search_entry("agatha christie", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "Styles"},
        ]),
    }
    result = find_author_search_entry(
        collected,
        search_query='author:"agatha christie"',
        sort="editions",
        allow_unsorted_fallback=True,
    )
    assert result is not None
    assert result["query"] == "agatha christie"


def test_find_author_search_entry_prefers_exact_sort_over_unsorted_fallback():
    collected = {
        "ol:search:unsorted": _make_search_entry("agatha christie", None, [
            {"key": "/works/OL1W", "rank": 1, "title": "Unsorted"},
        ]),
        "ol:search:sorted": _make_search_entry("agatha christie", "editions", [
            {"key": "/works/OL2W", "rank": 1, "title": "Sorted"},
        ]),
    }
    result = find_author_search_entry(
        collected,
        search_query='author:"agatha christie"',
        sort="editions",
        allow_unsorted_fallback=True,
    )
    assert result is not None
    assert result["sort"] == "editions"
    assert result["query"] == "agatha christie"


def test_find_author_search_entry_matches_plain_text_query():
    """Agent typed 'agatha christie' instead of 'author:\"agatha christie\"'."""
    collected = {
        "ol:search:christie": _make_search_entry("agatha christie", "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "Styles"},
        ]),
    }
    result = find_author_search_entry(
        collected, search_query='author:"agatha christie"', sort="editions",
    )
    assert result is not None
    assert result["query"] == "agatha christie"


def test_find_author_search_entry_plain_text_wrong_author_no_match():
    """Plain-text fallback must still reject a different author."""
    collected = {
        "ol:search:king": _make_search_entry("stephen king", "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It"},
        ]),
    }
    result = find_author_search_entry(
        collected, search_query='author:"agatha christie"', sort="editions",
    )
    assert result is None


def test_comparison_matches_when_second_author_uses_plain_text():
    """Regression: author_comparison must not return not_collected when the
    agent searches for the second author using plain text."""
    tmpl = OpenLibraryAuthorComparisonTemplate()
    collected = {
        "ol:search:king": _make_search_entry('author:"stephen king"', "editions", [
            {"key": "/works/OL1W", "rank": 1, "title": "It", "ratings_count": 500},
        ]),
        "ol:search:christie": _make_search_entry("agatha christie", "editions", [
            {"key": "/works/OL3W", "rank": 1, "title": "Styles", "ratings_count": 100},
        ]),
    }
    result = _run_gt(collected, tmpl.get_ground_truth({
        "author_a_name": "Stephen King",
        "author_a_query": "stephen king",
        "search_query_a": 'author:"stephen king"',
        "author_b_name": "Agatha Christie",
        "author_b_query": "agatha christie",
        "search_query_b": 'author:"agatha christie"',
        "sort": "editions", "work_count": 1, "metric": "ratings_count",
        "metric_label": "total number of ratings",
    }))
    assert result.success is True
    assert result.value == "Stephen King"


# ── 8. Cross-template consistency ─────────────────────────────────────


@pytest.mark.parametrize("cls", [
    OpenLibraryAuthorEngagementExtremaTemplate,
    OpenLibraryAuthorComparisonTemplate,
    OpenLibraryReadingStatsFilterTemplate,
])
def test_gt_source_is_page_only(cls):
    assert cls().get_gt_source() == GTSourceType.PAGE_ONLY


@pytest.mark.parametrize("cls", [
    OpenLibraryAuthorEngagementExtremaTemplate,
    OpenLibraryAuthorComparisonTemplate,
    OpenLibraryReadingStatsFilterTemplate,
])
def test_cache_source_is_openlibrary(cls):
    assert cls.get_cache_source() == "openlibrary"


def test_engagement_extrema_metrics_use_confirmed_visible_fields():
    metric_names = {m.value[0] for m in EngagementMetric}
    assert metric_names == {"want_to_read_count", "ratings_count"}


def test_author_comparison_metrics_use_confirmed_visible_fields():
    metric_names = {m.value[0] for m in AuthorMetric}
    assert metric_names == {"ratings_count", "want_to_read_count"}


def test_reading_filter_metrics_use_confirmed_visible_fields():
    metric_names = {m.value[0] for m in ReaderMetric}
    assert metric_names == {"want_to_read_count", "ratings_count"}


def test_all_new_templates_reuse_author_pool():
    from liveweb_arena.plugins.openlibrary.templates.author_engagement_extrema import AUTHOR_POOL as EX_POOL
    from liveweb_arena.plugins.openlibrary.templates.author_comparison import AUTHOR_POOL as CMP_POOL
    from liveweb_arena.plugins.openlibrary.templates.reading_stats_filter import AUTHOR_POOL as FLT_POOL
    assert EX_POOL is AUTHOR_POOL
    assert CMP_POOL is AUTHOR_POOL
    assert FLT_POOL is AUTHOR_POOL


def test_all_validation_info_values_are_serializable():
    templates = [
        OpenLibraryAuthorEngagementExtremaTemplate(),
        OpenLibraryAuthorComparisonTemplate(),
        OpenLibraryReadingStatsFilterTemplate(),
    ]
    for tmpl in templates:
        q = tmpl.generate(seed=1)
        for key, val in q.validation_info.items():
            assert isinstance(val, (str, int, float, bool, type(None))), (
                f"{tmpl.name}.validation_info['{key}'] = {type(val).__name__} "
                f"(not JSON-serializable)"
            )
