"""Weighted ranking template for Hacker News - HARD DIFFICULTY.

RL-friendly design:
- Requires collecting score and comment count for top-N stories
- Computes custom weighted scores and re-ranks stories
- Dynamic homepage values prevent memorization
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
from .common import (
    extract_first_number,
    get_homepage_stories,
    title_matches,
    title_partial_match,
)


class QueryType(Enum):
    """Question type for weighted ranking tasks."""

    STORY_AT_POSITION = "story_at_position"
    POSITION_OF_STORY = "position_of_story"


@register_template("hackernews_weighted_rank")
class HackerNewsWeightedRankTemplate(HackerNewsTemplateBase):
    """
    HARD: Re-rank top-N stories by a weighted formula.

    Weighted score: score + (weight_k * comments).
    Ties are broken by homepage rank (lower rank wins) for determinism.
    """

    STORY_COUNTS = [5, 7, 10, 12, 15]
    WEIGHTS = [1, 2, 3, 4, 5, 7, 10, 15]

    PATTERNS = {
        QueryType.STORY_AT_POSITION: [
            "Among the top {n} stories on HN, compute weighted score = points + ({k} x comments). Which story is ranked #{target} by this weighted score?",
            "On HN top {n}, re-rank stories using score + {k}*comments. Which title lands at weighted rank #{target}?",
            "Take the top {n} HN stories and sort by (score + {k}*comment_count). What is the story at position #{target}?",
        ],
        QueryType.POSITION_OF_STORY: [
            "Among the top {n} stories on HN, compute weighted score = points + ({k} x comments). What weighted rank does homepage story #{target} get?",
            "On HN top {n}, re-rank stories using score + {k}*comments. What position does original rank #{target} end up in?",
            "Take the top {n} HN stories and sort by (score + {k}*comment_count). What is the weighted position of story #{target} from the homepage?",
        ],
    }

    def __init__(self):
        super().__init__("hackernews_weighted_rank")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        rng = random.Random(seed)
        query_types = list(QueryType)

        if variant is not None:
            i = variant
            query_type = query_types[i % len(query_types)]
            i //= len(query_types)
            story_count = self.STORY_COUNTS[i % len(self.STORY_COUNTS)]
            i //= len(self.STORY_COUNTS)
            weight_k = self.WEIGHTS[i % len(self.WEIGHTS)]
            i //= len(self.WEIGHTS)
            target_rank = (i % story_count) + 1
        else:
            query_type = rng.choice(query_types)
            story_count = rng.choice(self.STORY_COUNTS)
            weight_k = rng.choice(self.WEIGHTS)
            target_rank = rng.randint(1, story_count)

        pattern = rng.choice(self.PATTERNS[query_type])
        question_text = pattern.format(n=story_count, k=weight_k, target=target_rank)

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://news.ycombinator.com/",
            variables={
                "query_type": query_type.value,
                "story_count": story_count,
                "weight_k": weight_k,
                "target_rank": target_rank,
            },
            validation_info={
                "query_type": query_type.value,
                "story_count": story_count,
                "weight_k": weight_k,
                "target_rank": target_rank,
            },
            template_name=self.name,
            expected_steps=story_count + 4,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        story_count = validation_info.get("story_count", 5)
        weight_k = validation_info.get("weight_k", 2)
        target_rank = validation_info.get("target_rank", 1)
        query_type = validation_info.get("query_type", "story_at_position")
        if query_type == "position_of_story":
            return f"""Task-Specific Rules (HN Weighted Rank):
- Analyze top {story_count} homepage stories on HN
- Weighted score formula: score + ({weight_k} x comments)
- Find weighted position of homepage story #{target_rank}
- Score 1.0: Exact position
- Score 0.5: Off by 1
- Score 0.0: Wrong position or no number"""
        return f"""Task-Specific Rules (HN Weighted Rank):
- Analyze top {story_count} homepage stories on HN
- Weighted score formula: score + ({weight_k} x comments)
- Return story title at weighted position #{target_rank}
- Score 1.0: Correct title (minor punctuation/casing differences allowed)
- Score 0.5: Partial title match
- Score 0.0: Wrong story title or no answer"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> GroundTruthResult:
        story_count = validation_info.get("story_count", 5)
        weight_k = validation_info.get("weight_k", 2)
        target_rank = validation_info.get("target_rank", 1)
        query_type = validation_info.get("query_type", "story_at_position")

        stories, failure = get_homepage_stories(
            story_count=story_count,
            required_fields=("rank", "title", "score", "descendants"),
            max_rank=story_count + 10,
        )
        if failure is not None:
            return failure

        weighted = []
        for story in stories:
            try:
                score = float(story["score"])
                comments = float(story["descendants"])
                rank = int(story["rank"])
            except (TypeError, ValueError, KeyError):
                return GroundTruthResult.system_error("Invalid story score/comment/rank type")
            weighted_score = score + (float(weight_k) * comments)
            weighted.append((weighted_score, rank, story))

        weighted.sort(key=lambda item: (-item[0], item[1]))

        if target_rank < 1 or target_rank > len(weighted):
            return GroundTruthResult.system_error(
                f"target_rank {target_rank} out of range for {len(weighted)} stories"
            )

        if query_type == "position_of_story":
            for pos, (_, original_rank, _) in enumerate(weighted, start=1):
                if original_rank == target_rank:
                    return GroundTruthResult.ok(str(pos))
            return GroundTruthResult.not_collected(
                f"Homepage rank #{target_rank} not in collected stories "
                f"(may be a job posting or missing data)"
            )

        chosen_story = weighted[target_rank - 1][2]
        title = chosen_story.get("title")
        if not isinstance(title, str) or not title.strip():
            return GroundTruthResult.system_error("Weighted-rank story has no title")
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

        expected = str(result.value)
        query_type = validation_info.get("query_type", "story_at_position")
        if query_type == "position_of_story":
            actual_num = extract_first_number(answer, signed=False, allow_float=False)
            if actual_num is None:
                return ValidationResult(
                    score=0.0,
                    is_correct=False,
                    expected=expected,
                    actual=answer,
                    details="No number found in answer",
                )
            actual_pos = int(actual_num)
            expected_pos = int(expected)
            if actual_pos == expected_pos:
                return ValidationResult(
                    score=1.0,
                    is_correct=True,
                    expected=expected,
                    actual=answer,
                    details="Exact weighted position match",
                )
            if abs(actual_pos - expected_pos) == 1:
                return ValidationResult(
                    score=0.5,
                    is_correct=False,
                    expected=expected,
                    actual=answer,
                    details="Close weighted position (off by 1)",
                )
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=expected,
                actual=answer,
                details=f"Wrong weighted position: expected {expected_pos}, got {actual_pos}",
            )

        if title_matches(expected, answer):
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=expected,
                actual=answer,
                details="Correct weighted-rank story title",
            )

        if title_partial_match(expected, answer, min_ratio=0.6):
            return ValidationResult(
                score=0.5,
                is_correct=False,
                expected=expected,
                actual=answer,
                details="Partial weighted-rank title match",
            )

        return ValidationResult(
            score=0.0,
            is_correct=False,
            expected=expected,
            actual=answer,
            details="Wrong weighted-rank story title",
        )
