"""Tests for new Hacker News templates: derived_metric and weighted_rank."""

import asyncio
from typing import Any, Dict

import pytest

from liveweb_arena.core.gt_collector import GTSourceType, set_current_gt_collector
from liveweb_arena.core.task_registry import TaskRegistry
from liveweb_arena.core.validators.base import get_registered_templates
from liveweb_arena.plugins.hackernews.templates.derived_metric import (
    HackerNewsDerivedMetricTemplate,
)
from liveweb_arena.plugins.hackernews.templates.weighted_rank import (
    HackerNewsWeightedRankTemplate,
)


class _DummyCollector:
    def __init__(self, data: Dict[str, Dict[str, Any]]):
        self._data = data

    def get_collected_api_data(self) -> Dict[str, Dict[str, Any]]:
        return self._data


def _run_with_collector(data: Dict[str, Dict[str, Any]], coro):
    set_current_gt_collector(_DummyCollector(data))
    try:
        return asyncio.run(coro)
    finally:
        set_current_gt_collector(None)


def _make_story(rank: int, title: str, score: int, comments: int) -> Dict[str, Any]:
    return {
        "id": 1000 + rank,
        "rank": rank,
        "title": title,
        "score": score,
        "descendants": comments,
    }


# ── Registration & wiring ────────────────────────────────────────────

def test_templates_registered():
    templates = get_registered_templates()
    assert "hackernews_derived_metric" in templates
    assert "hackernews_weighted_rank" in templates


def test_registry_ids_and_version():
    assert TaskRegistry.TEMPLATES[110] == ("hackernews", "hackernews_derived_metric")
    assert TaskRegistry.TEMPLATES[111] == ("hackernews", "hackernews_weighted_rank")
    assert any(sorted(v) == [110, 111] for v in TaskRegistry.TEMPLATE_VERSIONS)


@pytest.mark.parametrize("cls", [
    HackerNewsDerivedMetricTemplate,
    HackerNewsWeightedRankTemplate,
])
def test_template_sources(cls):
    tmpl = cls()
    assert tmpl.get_gt_source() == GTSourceType.PAGE_ONLY
    assert cls.get_cache_source() == "hackernews"


@pytest.mark.parametrize("cls", [
    HackerNewsDerivedMetricTemplate,
    HackerNewsWeightedRankTemplate,
])
def test_validation_info_serializable(cls):
    q = cls().generate(seed=42)
    for key, value in q.validation_info.items():
        assert isinstance(value, (str, int, float, bool, type(None))), (
            f"{cls.__name__}.validation_info['{key}'] not JSON-serializable"
        )


# ── Variant space ────────────────────────────────────────────────────

def test_derived_metric_variant_space_exceeds_500():
    t = HackerNewsDerivedMetricTemplate
    total = (
        len(t.STORY_COUNTS)
        * len(t.WINDOW_SIZES)
        * len(t.WINDOW_STARTS)
        * len(t.SMOOTHING_K)
        * len(t.DENOM_POWERS)
        * 2  # metrics
        * 2  # directions
    )
    assert total > 500, f"derived_metric variant space {total} <= 500"


def test_weighted_rank_variant_space_exceeds_500():
    t = HackerNewsWeightedRankTemplate
    total = 2 * len(t.WEIGHTS) * sum(t.STORY_COUNTS)  # query_types × weights × Σ(story_counts)
    assert total > 500, f"weighted_rank variant space {total} <= 500"


def test_derived_metric_variant_enumerates_all_params():
    """Variant index must cycle through all underlying parameter axes."""
    tmpl = HackerNewsDerivedMetricTemplate()
    seen = set()
    for v in range(100):
        q = tmpl.generate(seed=0, variant=v)
        vi = q.validation_info
        key = (
            vi["metric"], vi["direction"], vi["story_count"],
            vi["window_start"], vi["window_end"],
            vi["smoothing_k"], vi["denominator_power"],
        )
        seen.add(key)
    # Some (window_start, window_size) pairs produce the same window_end,
    # so perfect uniqueness isn't guaranteed; but diversity must be high.
    assert len(seen) >= 90


def test_weighted_rank_variant_enumerates_all_params():
    """Each variant index must produce a unique parameter combination."""
    tmpl = HackerNewsWeightedRankTemplate()
    seen = set()
    for v in range(80):
        q = tmpl.generate(seed=0, variant=v)
        vi = q.validation_info
        key = (vi["query_type"], vi["story_count"], vi["weight_k"], vi["target_rank"])
        seen.add(key)
    assert len(seen) == 80


# ── derived_metric GT ────────────────────────────────────────────────

def test_derived_metric_highest_comments_per_point():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 100),   # 1.00
        "12": _make_story(2, "Beta", 50, 120),     # 2.40
        "13": _make_story(3, "Gamma", 80, 20),     # 0.25
        "14": _make_story(4, "Delta", 60, 60),     # 1.00
        "15": _make_story(5, "Epsilon", 40, 10),   # 0.25
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 5,
        "window_start": 1,
        "window_end": 5,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    assert result.value == "Beta"


def test_derived_metric_lowest_points_per_comment():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 50),    # 2.0
        "12": _make_story(2, "Beta", 30, 120),     # 0.25
        "13": _make_story(3, "Gamma", 80, 20),     # 4.0
        "14": _make_story(4, "Delta", 60, 60),     # 1.0
        "15": _make_story(5, "Epsilon", 40, 10),   # 4.0
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "points_per_comment",
        "metric_label": "points per comment",
        "numerator_field": "score",
        "denominator_field": "descendants",
        "direction": "lowest",
        "story_count": 5,
        "window_start": 1,
        "window_end": 5,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    assert result.value == "Beta"


def test_derived_metric_zero_denominator_deterministic():
    """Zero-denominator stories produce inf; tie-break by rank."""
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 0, 100),
        "12": _make_story(2, "Beta", 0, 80),
        "13": _make_story(3, "Gamma", 0, 10),
        "14": _make_story(4, "Delta", 10, 60),
        "15": _make_story(5, "Epsilon", 20, 90),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 5,
        "window_start": 1,
        "window_end": 5,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    assert result.value == "Alpha"


def test_derived_metric_invalid_numeric_returns_system_error():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": {"id": 1011, "rank": 1, "title": "Alpha", "score": "bad", "descendants": 100},
        "12": _make_story(2, "Beta", 50, 120),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 60, 60),
        "15": _make_story(5, "Epsilon", 40, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 5,
        "window_start": 1,
        "window_end": 5,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is False
    assert "Invalid numeric fields" in (result.error or "")
    assert result.is_system_error()


def test_derived_metric_window_filters_ranks():
    """Only stories within the rank window should be considered."""
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 10, 100),    # 10.0 (outside window)
        "12": _make_story(2, "Beta", 50, 120),     # 2.40
        "13": _make_story(3, "Gamma", 80, 200),    # 2.50
        "14": _make_story(4, "Delta", 60, 60),     # 1.00
        "15": _make_story(5, "Epsilon", 40, 10),   # 0.25
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 5,
        "window_start": 2,
        "window_end": 5,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    assert result.value == "Gamma"


def test_derived_metric_smoothing_k_flips_winner():
    """Smoothing k shifts the denominator enough to change the winner."""
    tmpl = HackerNewsDerivedMetricTemplate()
    # k=0:   Alpha=10/1=10 (winner), Beta=100/90≈1.11
    # k=100: Alpha=10/101≈0.099, Beta=100/190≈0.526 (winner)
    collected = {
        "11": _make_story(1, "Alpha", 1, 10),
        "12": _make_story(2, "Beta", 90, 100),
        "13": _make_story(3, "Gamma", 80, 20),
    }
    result_no_smooth = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 3,
        "window_start": 1,
        "window_end": 3,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    result_with_smooth = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 3,
        "window_start": 1,
        "window_end": 3,
        "smoothing_k": 100,
        "denominator_power": 1.0,
    }))
    assert result_no_smooth.success and result_with_smooth.success
    assert result_no_smooth.value == "Alpha"
    assert result_with_smooth.value == "Beta"


def test_derived_metric_denominator_power_affects_result():
    """Increasing denominator power flips the winner."""
    tmpl = HackerNewsDerivedMetricTemplate()
    # Alpha: score=100, comments=500 → p=1.0: 500/100=5.0 (winner)
    #                                   p=1.5: 500/(100^1.5)=500/1000=0.5
    # Beta:  score=1,   comments=1   → p=1.0: 1/1=1.0
    #                                   p=1.5: 1/(1^1.5)=1/1=1.0 (winner)
    collected = {
        "11": _make_story(1, "Alpha", 100, 500),
        "12": _make_story(2, "Beta", 1, 1),
        "13": _make_story(3, "Gamma", 40, 5),
    }
    result_p1 = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 3,
        "window_start": 1,
        "window_end": 3,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    result_p15 = _run_with_collector(collected, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 3,
        "window_start": 1,
        "window_end": 3,
        "smoothing_k": 0,
        "denominator_power": 1.5,
    }))
    assert result_p1.success and result_p15.success
    assert result_p1.value == "Alpha"  # 500/100 = 5.0 > 1/1 = 1.0
    assert result_p15.value == "Beta"  # 1/(1^1.5) = 1.0 > 500/(100^1.5) = 0.5


def test_derived_metric_validate_correct_title():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha Story", 100, 100),
        "12": _make_story(2, "Beta Story", 50, 120),
        "13": _make_story(3, "Gamma Story", 80, 20),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Beta Story",
        {
            "metric": "comments_per_point",
            "metric_label": "comments per point",
            "numerator_field": "descendants",
            "denominator_field": "score",
            "direction": "highest",
            "story_count": 3,
            "window_start": 1,
            "window_end": 3,
            "smoothing_k": 0,
            "denominator_power": 1.0,
        },
    ))
    assert result.score == 1.0
    assert result.is_correct is True


def test_derived_metric_validate_partial_title():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha Story Title Here", 100, 100),
        "12": _make_story(2, "Beta Story Title Here", 50, 120),
        "13": _make_story(3, "Gamma Story Title Here", 80, 20),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "I found Beta Story Title",
        {
            "metric": "comments_per_point",
            "metric_label": "comments per point",
            "numerator_field": "descendants",
            "denominator_field": "score",
            "direction": "highest",
            "story_count": 3,
            "window_start": 1,
            "window_end": 3,
            "smoothing_k": 0,
            "denominator_power": 1.0,
        },
    ))
    assert result.score == 0.5


def test_derived_metric_validate_wrong_title():
    tmpl = HackerNewsDerivedMetricTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 100),
        "12": _make_story(2, "Beta", 50, 120),
        "13": _make_story(3, "Gamma", 80, 20),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Gamma",
        {
            "metric": "comments_per_point",
            "metric_label": "comments per point",
            "numerator_field": "descendants",
            "denominator_field": "score",
            "direction": "highest",
            "story_count": 3,
            "window_start": 1,
            "window_end": 3,
            "smoothing_k": 0,
            "denominator_power": 1.0,
        },
    ))
    assert result.score == 0.0
    assert result.is_correct is False


# ── weighted_rank GT ─────────────────────────────────────────────────

def test_weighted_rank_story_at_position():
    tmpl = HackerNewsWeightedRankTemplate()
    # k=5: Alpha=150, Beta=240, Gamma=180, Delta=170, Epsilon=110
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 5,
        "weight_k": 5,
        "target_rank": 2,
    }))
    assert result.success is True
    assert result.value == "Gamma"


def test_weighted_rank_position_of_story():
    tmpl = HackerNewsWeightedRankTemplate()
    # k=5: Alpha=150, Beta=240, Gamma=180, Delta=170, Epsilon=110
    # Weighted order: Beta(1), Gamma(2), Delta(3), Alpha(4), Epsilon(5)
    # Homepage rank #3 (Gamma) → weighted position 2
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "query_type": "position_of_story",
        "story_count": 5,
        "weight_k": 5,
        "target_rank": 3,
    }))
    assert result.success is True
    assert result.value == "2"


def test_weighted_rank_tie_break_by_homepage_rank():
    """Tied weighted scores must break by lower homepage rank."""
    tmpl = HackerNewsWeightedRankTemplate()
    # k=2: Alpha=120, Beta=120, Gamma=110
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 15),
        "13": _make_story(3, "Gamma", 80, 15),
        "14": _make_story(4, "Delta", 70, 10),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "query_type": "position_of_story",
        "story_count": 5,
        "weight_k": 2,
        "target_rank": 2,
    }))
    assert result.success is True
    assert result.value == "2"


def test_weighted_rank_validate_exact_position():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Position is 2",
        {
            "query_type": "position_of_story",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 3,
        },
    ))
    assert result.score == 1.0
    assert result.is_correct is True


def test_weighted_rank_validate_off_by_one():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Position is 3",
        {
            "query_type": "position_of_story",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 3,
        },
    ))
    assert result.score == 0.5


def test_weighted_rank_validate_wrong_position():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Position is 5",
        {
            "query_type": "position_of_story",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 3,
        },
    ))
    assert result.score == 0.0


def test_weighted_rank_validate_no_number():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "I don't know",
        {
            "query_type": "position_of_story",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 3,
        },
    ))
    assert result.score == 0.0
    assert "No number" in result.details


def test_weighted_rank_invalid_numeric_returns_system_error():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": {"id": 1012, "rank": 2, "title": "Beta", "score": "bad", "descendants": 30},
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 5,
        "weight_k": 5,
        "target_rank": 1,
    }))
    assert result.success is False
    assert "Invalid story" in (result.error or "")
    assert result.is_system_error()


def test_weighted_rank_validate_correct_title():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Gamma",
        {
            "query_type": "story_at_position",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 2,
        },
    ))
    assert result.score == 1.0
    assert result.is_correct is True


def test_weighted_rank_validate_partial_title():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha Story Title Here", 100, 10),
        "12": _make_story(2, "Beta Story Title Here", 90, 30),
        "13": _make_story(3, "Gamma Story Title Here", 80, 20),
        "14": _make_story(4, "Delta Story Title Here", 70, 20),
        "15": _make_story(5, "Epsilon Story Title Here", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "I found Gamma Story Title",
        {
            "query_type": "story_at_position",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 2,
        },
    ))
    assert result.score == 0.5


def test_weighted_rank_validate_wrong_title():
    tmpl = HackerNewsWeightedRankTemplate()
    collected = {
        "11": _make_story(1, "Alpha", 100, 10),
        "12": _make_story(2, "Beta", 90, 30),
        "13": _make_story(3, "Gamma", 80, 20),
        "14": _make_story(4, "Delta", 70, 20),
        "15": _make_story(5, "Epsilon", 60, 10),
    }
    result = _run_with_collector(collected, tmpl.validate_answer(
        "Alpha",
        {
            "query_type": "story_at_position",
            "story_count": 5,
            "weight_k": 5,
            "target_rank": 2,
        },
    ))
    assert result.score == 0.0
