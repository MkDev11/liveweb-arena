"""End-to-end GT computation tests using Hacker News API data.

Ranks 1-8 and 10-15: real data fetched live on April 2, 2026 via:
  https://hacker-news.firebaseio.com/v0/topstories.json
  https://hacker-news.firebaseio.com/v0/item/{id}.json
Rank 9 was a job posting (excluded — not a story record).
Ranks 16-20: synthetic entries added to support story_count=15 tests.

These tests verify CLAUDE.md §5 item 1: "GT must return a concrete value."
They inject API-shaped data (field names, nesting, types) into the
GT collector and confirm each template computes a concrete answer.
"""

import asyncio
from typing import Any, Dict

from liveweb_arena.core.gt_collector import set_current_gt_collector
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


def _run_gt(data: Dict[str, Dict[str, Any]], coro):
    set_current_gt_collector(_DummyCollector(data))
    try:
        return asyncio.run(coro)
    finally:
        set_current_gt_collector(None)


# ── Fixture data ──────────────────────────────────────────────────────
# Ranks 1-8, 10-15: real API data (April 2, 2026 HN Firebase API).
# Rank 9: job posting (excluded — not a story record).
# Ranks 16-20: synthetic entries for story_count=15 coverage.

REAL_HN_STORIES = {
    "47603657": {
        "id": 47603657, "rank": 1,
        "title": "Live: Artemis II Launch Day Updates",
        "score": 905, "descendants": 773,
        "url": "https://www.nasa.gov/blogs/missions/2026/04/01/live-artemis-ii-launch-day-updates/",
        "by": "apitman", "type": "story",
    },
    "47611481": {
        "id": 47611481, "rank": 2,
        "title": "Bringing Clojure programming to Enterprise (2021)",
        "score": 10, "descendants": 1,
        "url": "https://blogit.michelin.io/clojure-programming/",
        "by": "smartmic", "type": "story",
    },
    "47609694": {
        "id": 47609694, "rank": 3,
        "title": "Email obfuscation: What works in 2026?",
        "score": 96, "descendants": 23,
        "url": "https://spencermortensen.com/articles/email-obfuscation/",
        "by": "jaden", "type": "story",
    },
    "47609564": {
        "id": 47609564, "rank": 4,
        "title": "Steam on Linux Use Skyrocketed Above 5% in March",
        "score": 321, "descendants": 134,
        "url": "https://www.phoronix.com/news/Steam-On-Linux-Tops-5p",
        "by": "hkmaxpro", "type": "story",
    },
    "47608495": {
        "id": 47608495, "rank": 5,
        "title": "Quantum computing bombshells that are not April Fools",
        "score": 153, "descendants": 49,
        "url": "https://scottaaronson.blog/?p=9665",
        "by": "Strilanc", "type": "story",
    },
    "47602832": {
        "id": 47602832, "rank": 6,
        "title": "EmDash \u2013 A spiritual successor to WordPress that solves plugin security",
        "score": 557, "descendants": 402,
        "url": "https://blog.cloudflare.com/emdash-wordpress/",
        "by": "elithrar", "type": "story",
    },
    "47608058": {
        "id": 47608058, "rank": 7,
        "title": "A new C++ back end for ocamlc",
        "score": 172, "descendants": 14,
        "url": "https://github.com/ocaml/ocaml/pull/14701",
        "by": "glittershark", "type": "story",
    },
    "47611500": {
        "id": 47611500, "rank": 8,
        "title": "Should AI have the right to say 'No' to its owner?",
        "score": 4, "descendants": 2,
        "url": "https://github.com/Jang-woo-AnnaSoft/execution-boundaries",
        "by": "Jang-woo", "type": "story",
    },
    "47596739": {
        "id": 47596739, "rank": 10,
        "title": "Mercor says it was hit by cyberattack tied to compromise LiteLLM",
        "score": 31, "descendants": 9,
        "url": "https://techcrunch.com/2026/03/31/mercor-cyberattack/",
        "by": "jackson-mcd", "type": "story",
    },
    "47558531": {
        "id": 47558531, "rank": 11,
        "title": "AI Perfected Chess. Humans Made It Unpredictable Again",
        "score": 30, "descendants": 22,
        "url": "https://www.bloomberg.com/news/articles/2026-03-27/ai-chess",
        "by": "GMoromisato", "type": "story",
    },
    "47606840": {
        "id": 47606840, "rank": 12,
        "title": "DRAM pricing is killing the hobbyist SBC market",
        "score": 461, "descendants": 391,
        "url": "https://www.jeffgeerling.com/blog/2026/dram-pricing/",
        "by": "ingve", "type": "story",
    },
    "47584386": {
        "id": 47584386, "rank": 13,
        "title": "Fast and Gorgeous Erosion Filter",
        "score": 149, "descendants": 14,
        "url": "https://blog.runevision.com/2026/03/erosion-filter.html",
        "by": "runevision", "type": "story",
    },
    "47557921": {
        "id": 47557921, "rank": 14,
        "title": "Show HN: Git bayesect \u2013 Bayesian Git bisection for non-deterministic bugs",
        "score": 272, "descendants": 40,
        "url": "https://github.com/hauntsaninja/git_bayesect",
        "by": "hauntsaninja", "type": "story",
    },
    "47609725": {
        "id": 47609725, "rank": 15,
        "title": "Show HN: NASA Artemis II Mission Timeline Tracker",
        "score": 44, "descendants": 7,
        "url": "https://www.sunnywingsvirtual.com/artemis2/timeline.html",
        "by": "AustinDev", "type": "story",
    },
    # Ranks 16-20: additional stories to support story_count=15 tests.
    # Plausible HN entries with decreasing scores consistent with the snapshot.
    "47605100": {
        "id": 47605100, "rank": 16,
        "title": "Why SQLite is the only database you will ever need",
        "score": 38, "descendants": 19,
        "url": "https://blog.example.com/sqlite-all-you-need",
        "by": "sqliteFan", "type": "story",
    },
    "47604200": {
        "id": 47604200, "rank": 17,
        "title": "A visual guide to SSH tunnels",
        "score": 35, "descendants": 5,
        "url": "https://blog.example.com/ssh-tunnels-visual",
        "by": "netadmin42", "type": "story",
    },
    "47603300": {
        "id": 47603300, "rank": 18,
        "title": "Show HN: Tiny Rust HTTP server in 100 lines",
        "score": 28, "descendants": 12,
        "url": "https://github.com/example/tiny-http-rs",
        "by": "rustdev", "type": "story",
    },
    "47602400": {
        "id": 47602400, "rank": 19,
        "title": "The hidden cost of microservices",
        "score": 22, "descendants": 31,
        "url": "https://blog.example.com/microservices-cost",
        "by": "archreview", "type": "story",
    },
    "47601500": {
        "id": 47601500, "rank": 20,
        "title": "Firefox 140 released with major performance improvements",
        "score": 19, "descendants": 8,
        "url": "https://www.mozilla.org/firefox/140.0/releasenotes/",
        "by": "nickcox", "type": "story",
    },
}


# ── derived_metric GT tests ───────────────────────────────────────────

def test_derived_metric_highest_comments_per_point_real_data():
    """Top 8 stories, highest comments/score, no smoothing."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 8,
        "window_start": 1,
        "window_end": 8,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    # Artemis=773/905=0.854 (highest), EmDash=402/557=0.722,
    # AI-No=2/4=0.5, Steam=134/321=0.417
    assert result.value == "Live: Artemis II Launch Day Updates"


def test_derived_metric_lowest_points_per_comment_real_data():
    """Top 8 stories, lowest score/comments."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "points_per_comment",
        "metric_label": "points per comment",
        "numerator_field": "score",
        "denominator_field": "descendants",
        "direction": "lowest",
        "story_count": 8,
        "window_start": 1,
        "window_end": 8,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    # Artemis=905/773=1.17 (lowest), EmDash=557/402=1.39, AI-No=4/2=2.0
    assert result.value == "Live: Artemis II Launch Day Updates"


def test_derived_metric_with_smoothing_real_data():
    """Smoothing k=20 with real data returns concrete GT."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 8,
        "window_start": 1,
        "window_end": 8,
        "smoothing_k": 20,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    assert isinstance(result.value, str) and len(result.value) > 0


def test_derived_metric_with_denom_power_real_data():
    """Denominator power=1.5 changes winner with real data."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 8,
        "window_start": 1,
        "window_end": 8,
        "smoothing_k": 0,
        "denominator_power": 1.5,
    }))
    assert result.success is True
    # power=1.5: AI-No=2/(4^1.5)=2/8=0.25 beats Artemis=773/(905^1.5)=0.028
    assert result.value == "Should AI have the right to say 'No' to its owner?"


def test_derived_metric_window_filter_real_data():
    """Window #3-#7 excludes ranks 1,2,8 — changes winner."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 8,
        "window_start": 3,
        "window_end": 7,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    # Window #3-#7: EmDash=402/557=0.722 (highest, Artemis excluded)
    assert result.value == "EmDash \u2013 A spiritual successor to WordPress that solves plugin security"


# ── weighted_rank GT tests ────────────────────────────────────────────

def test_weighted_rank_story_at_position_real_data():
    """Weighted rank with k=2, top 8, position 1."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 8,
        "weight_k": 2,
        "target_rank": 1,
    }))
    assert result.success is True
    # Artemis=905+2*773=2451 (position 1)
    assert result.value == "Live: Artemis II Launch Day Updates"


def test_weighted_rank_position_of_story_real_data():
    """Position of homepage rank #6 (EmDash) in weighted ranking."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "position_of_story",
        "story_count": 8,
        "weight_k": 2,
        "target_rank": 6,
    }))
    assert result.success is True
    # EmDash weighted=1361 → position 2
    assert result.value == "2"


def test_weighted_rank_different_weight_real_data():
    """Weight k=10 heavily favors comments."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 8,
        "weight_k": 10,
        "target_rank": 2,
    }))
    assert result.success is True
    # k=10: EmDash=557+10*402=4577 (position 2)
    assert result.value == "EmDash \u2013 A spiritual successor to WordPress that solves plugin security"


def test_weighted_rank_position_3_real_data():
    """Weight k=10, position 3."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 8,
        "weight_k": 10,
        "target_rank": 3,
    }))
    assert result.success is True
    # k=10: Steam=321+10*134=1661 (position 3)
    assert result.value == "Steam on Linux Use Skyrocketed Above 5% in March"


# ── Rank-gap tolerance tests (rank 9 = job posting, excluded) ─────────
# The real snapshot has no rank 9 (it was a job posting without descendants).
# These tests verify that story_count >= 10 works via max_rank tolerance.

def test_derived_metric_story_count_10_skips_job_rank():
    """story_count=10 succeeds despite rank-9 gap (job posting)."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "comments_per_point",
        "metric_label": "comments per point",
        "numerator_field": "descendants",
        "denominator_field": "score",
        "direction": "highest",
        "story_count": 10,
        "window_start": 1,
        "window_end": 10,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    # 10 stories collected (ranks 1-8, 10, 11), rank 9 skipped
    # Artemis still highest at 773/905=0.854
    assert result.value == "Live: Artemis II Launch Day Updates"


def test_weighted_rank_story_count_12_skips_job_rank():
    """story_count=12 succeeds despite rank-9 gap (job posting)."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 12,
        "weight_k": 5,
        "target_rank": 3,
    }))
    assert result.success is True
    # 12 stories: ranks 1-8, 10-13 (rank 9 skipped)
    # k=5: Artemis=905+5*773=4770, EmDash=557+5*402=2567,
    #   DRAM=461+5*391=2416, Steam=321+5*134=991
    # Position 3: DRAM (2416)
    assert result.value == "DRAM pricing is killing the hobbyist SBC market"


def test_weighted_rank_position_of_missing_rank_returns_not_collected():
    """position_of_story with target_rank=9 (job posting) returns not_collected."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "position_of_story",
        "story_count": 10,
        "weight_k": 2,
        "target_rank": 9,
    }))
    assert result.success is False
    assert not result.is_system_error()
    assert "not in collected stories" in (result.error or "")


def test_weighted_rank_story_count_15_succeeds_with_expanded_data():
    """story_count=15 succeeds now that ranks 16+ exist in fixture."""
    tmpl = HackerNewsWeightedRankTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "query_type": "story_at_position",
        "story_count": 15,
        "weight_k": 5,
        "target_rank": 1,
    }))
    assert result.success is True
    # k=5: Artemis=905+5*773=4770 (position 1)
    assert result.value == "Live: Artemis II Launch Day Updates"


def test_derived_metric_story_count_15_window_excludes_gap():
    """story_count=15 with window #10-#15 excludes the gap entirely."""
    tmpl = HackerNewsDerivedMetricTemplate()
    result = _run_gt(REAL_HN_STORIES, tmpl.get_ground_truth({
        "metric": "points_per_comment",
        "metric_label": "points per comment",
        "numerator_field": "score",
        "denominator_field": "descendants",
        "direction": "highest",
        "story_count": 15,
        "window_start": 10,
        "window_end": 15,
        "smoothing_k": 0,
        "denominator_power": 1.0,
    }))
    assert result.success is True
    # Window #10-#15 (rank 9 gap excluded): Erosion=149/14=10.64 (highest)
    assert result.value == "Fast and Gorgeous Erosion Filter"
