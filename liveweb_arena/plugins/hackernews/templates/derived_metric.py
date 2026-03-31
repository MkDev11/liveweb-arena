"""Derived metric template for Hacker News - HARD DIFFICULTY.

RL-friendly design:
- Requires visiting top-N stories and detail pages to collect score/comments
- Computes cross-field derived metrics not shown directly on page
- Uses dynamic homepage data, making memorization ineffective
"""

import random
from enum import Enum
from typing import Any, Dict, Optional

from liveweb_arena.core.ground_truth_trigger import (
    GroundTruthResult,
)
from liveweb_arena.core.validators.base import (
    GeneratedQuestion,
    ValidationResult,
    register_template,
)

from .base import HackerNewsTemplateBase
from .common import get_homepage_stories, title_matches, title_partial_match


class DerivedMetric(Enum):
    """Derived metrics computed from story score/comment fields."""

    COMMENTS_PER_POINT = (
        "comments_per_point",
        "comments per point",
        "descendants",
        "score",
    )
    POINTS_PER_COMMENT = (
        "points_per_comment",
        "points per comment",
        "score",
        "descendants",
    )

    def __init__(
        self,
        value: str,
        display_name: str,
        numerator_field: str,
        denominator_field: str,
    ):
        self._value_ = value
        self.display_name = display_name
        self.numerator_field = numerator_field
        self.denominator_field = denominator_field


class Direction(Enum):
    """Direction for extrema query."""

    HIGHEST = "highest"
    LOWEST = "lowest"


@register_template("hackernews_derived_metric")
class HackerNewsDerivedMetricTemplate(HackerNewsTemplateBase):
    """
    HARD: Find extrema by derived metric (ratio across two fields).

    This differs from extrema_comparison: it computes per-story derived values
    and asks for the story title at the extrema, not a numeric gap/ratio.
    """

    STORY_COUNTS = [8, 10, 12, 15]
    WINDOW_SIZES = [5, 7, 10]
    WINDOW_STARTS = [1, 2, 3, 4, 5]
    SMOOTHING_K = [0, 1, 2, 5, 10, 20]
    DENOM_POWERS = [1.0, 1.25, 1.5]

    PATTERNS = {
        (DerivedMetric.COMMENTS_PER_POINT, Direction.HIGHEST): [
            "Among the top {n} stories on Hacker News, which story has the highest comments-per-point ratio?",
            "Look at the top {n} HN stories. Which title has the highest value of comments divided by score?",
            "On HN, within the top {n} stories, which one has the greatest comments-to-score ratio?",
        ],
        (DerivedMetric.COMMENTS_PER_POINT, Direction.LOWEST): [
            "Among the top {n} stories on Hacker News, which story has the lowest comments-per-point ratio?",
            "Look at the top {n} HN stories. Which title has the lowest value of comments divided by score?",
            "On HN, within the top {n} stories, which one has the smallest comments-to-score ratio?",
        ],
        (DerivedMetric.POINTS_PER_COMMENT, Direction.HIGHEST): [
            "Among the top {n} stories on Hacker News, which story has the highest points-per-comment ratio?",
            "Look at the top {n} HN stories. Which title has the highest value of score divided by comments?",
            "On HN, within the top {n} stories, which one has the greatest score-to-comments ratio?",
        ],
        (DerivedMetric.POINTS_PER_COMMENT, Direction.LOWEST): [
            "Among the top {n} stories on Hacker News, which story has the lowest points-per-comment ratio?",
            "Look at the top {n} HN stories. Which title has the lowest value of score divided by comments?",
            "On HN, within the top {n} stories, which one has the smallest score-to-comments ratio?",
        ],
    }

    def __init__(self):
        super().__init__("hackernews_derived_metric")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)
        metrics = list(DerivedMetric)
        directions = list(Direction)

        if variant is not None:
            i = variant
            metric = metrics[i % len(metrics)]
            i //= len(metrics)
            direction = directions[i % len(directions)]
            i //= len(directions)
            n = self.STORY_COUNTS[i % len(self.STORY_COUNTS)]
            i //= len(self.STORY_COUNTS)
            window_size = self.WINDOW_SIZES[i % len(self.WINDOW_SIZES)]
            i //= len(self.WINDOW_SIZES)
            window_start = self.WINDOW_STARTS[i % len(self.WINDOW_STARTS)]
            i //= len(self.WINDOW_STARTS)
            smoothing_k = self.SMOOTHING_K[i % len(self.SMOOTHING_K)]
            i //= len(self.SMOOTHING_K)
            denom_power = self.DENOM_POWERS[i % len(self.DENOM_POWERS)]
        else:
            metric = rng.choice(metrics)
            direction = rng.choice(directions)
            n = rng.choice(self.STORY_COUNTS)
            window_size = rng.choice(self.WINDOW_SIZES)
            window_start = rng.choice(self.WINDOW_STARTS)
            smoothing_k = rng.choice(self.SMOOTHING_K)
            denom_power = rng.choice(self.DENOM_POWERS)

        window_end = min(n, window_start + window_size - 1)
        if window_start > window_end:
            window_start = 1
            window_end = n

        pattern = rng.choice(self.PATTERNS[(metric, direction)])
        question_text = (
            f"{pattern.format(n=n)} "
            f"Only consider homepage ranks #{window_start}-#{window_end}. "
            f"Use derived score = numerator / ((denominator + {smoothing_k})^{denom_power})."
        )

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://news.ycombinator.com/",
            variables={"metric": metric.value, "direction": direction.value, "n": n},
            validation_info={
                "metric": metric.value,
                "metric_label": metric.display_name,
                "numerator_field": metric.numerator_field,
                "denominator_field": metric.denominator_field,
                "direction": direction.value,
                "story_count": n,
                "window_size": window_size,
                "window_start": window_start,
                "window_end": window_end,
                "smoothing_k": smoothing_k,
                "denominator_power": denom_power,
            },
            template_name=self.name,
            expected_steps=n + 3,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        n = validation_info.get("story_count", 5)
        metric_label = validation_info.get("metric_label", "")
        direction = validation_info.get("direction", "")
        return f"""Task-Specific Rules (HN Derived Metric):
- Analyze top {n} stories on HN homepage
- Compute {metric_label} for each story
- Return the story with the {direction} derived metric
- Score 1.0: Correct story title (minor punctuation/casing differences allowed)
- Score 0.5: Partially correct title reference
- Score 0.0: Wrong story or no answer"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        story_count = validation_info.get("story_count", 5)
        numerator_field = validation_info.get("numerator_field", "descendants")
        denominator_field = validation_info.get("denominator_field", "score")
        direction = validation_info.get("direction", "highest")
        window_start = int(validation_info.get("window_start", 1))
        window_end = int(validation_info.get("window_end", story_count))
        smoothing_k = float(validation_info.get("smoothing_k", 0))
        denominator_power = float(validation_info.get("denominator_power", 1.0))

        stories, failure = get_homepage_stories(
            story_count=story_count,
            required_fields=("title", numerator_field, denominator_field),
            max_rank=story_count + 10,
        )
        if failure is not None:
            return failure

        windowed_stories = [
            story for story in stories
            if isinstance(story.get("rank"), int) and window_start <= story["rank"] <= window_end
        ]
        if not windowed_stories:
            return GroundTruthResult.not_collected(
                f"No stories available in rank window #{window_start}-#{window_end}"
            )

        valid = []
        for story in windowed_stories:
            numerator = story.get(numerator_field)
            denominator = story.get(denominator_field)
            try:
                numerator_val = float(numerator)
                denominator_val = float(denominator)
            except (TypeError, ValueError):
                rank = story.get("rank", "?")
                return GroundTruthResult.system_error(
                    f"Invalid numeric fields for story rank {rank}"
                )
            adjusted_denominator = denominator_val + smoothing_k
            if adjusted_denominator < 0:
                rank = story.get("rank", "?")
                return GroundTruthResult.system_error(
                    f"Adjusted denominator below zero at rank {rank}"
                )
            if adjusted_denominator == 0:
                if numerator_val > 0:
                    ratio = float("inf")
                elif numerator_val < 0:
                    ratio = float("-inf")
                else:
                    ratio = 0.0
            else:
                ratio = numerator_val / (adjusted_denominator ** denominator_power)
            valid.append((story, ratio))

        if not valid:
            return GroundTruthResult.fail(
                "No valid stories for derived metric computation"
            )

        if direction == "lowest":
            chosen_story, _ = min(valid, key=lambda item: (item[1], item[0].get("rank", 9999)))
        else:
            chosen_story, _ = max(valid, key=lambda item: (item[1], -item[0].get("rank", 9999)))

        title = chosen_story.get("title")
        if not isinstance(title, str) or not title.strip():
            return GroundTruthResult.system_error("Derived metric winner has no title")
        return GroundTruthResult.ok(title.strip())

    async def validate_answer(
        self, answer: str, validation_info: Dict[str, Any]
    ) -> ValidationResult:
        result = await self.get_ground_truth(validation_info)
        if not result.success:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details=f"Ground truth unavailable: {result.error}",
            )

        expected_title = str(result.value)
        if title_matches(expected_title, answer):
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=expected_title,
                actual=answer,
                details="Correct story title",
            )

        if title_partial_match(expected_title, answer, min_ratio=0.6):
            return ValidationResult(
                score=0.5,
                is_correct=False,
                expected=expected_title,
                actual=answer,
                details="Partial title match",
            )

        return ValidationResult(
            score=0.0,
            is_correct=False,
            expected=expected_title,
            actual=answer,
            details="Wrong story title",
        )
