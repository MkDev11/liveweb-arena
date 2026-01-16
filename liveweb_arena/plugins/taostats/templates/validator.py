"""Validator query template for Taostats"""

import random
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
    Variable, VariableType
)


class ValidatorMetric(Enum):
    """Metrics available on taostats.io/validators"""
    TOP_BY_STAKE = "top_by_stake"
    TOP_BY_NOMINATIONS = "top_by_nominations"
    TOP_BY_DOMINANCE = "top_by_dominance"


@dataclass
class ValidatorMetricSpec:
    """Specification for a validator metric"""
    metric: ValidatorMetric
    display_name: str


class ValidatorMetricVariable(Variable):
    """Variable for validator metric selection"""

    METRICS: Dict[ValidatorMetric, ValidatorMetricSpec] = {
        ValidatorMetric.TOP_BY_STAKE: ValidatorMetricSpec(
            ValidatorMetric.TOP_BY_STAKE, "top validators by root stake"
        ),
        ValidatorMetric.TOP_BY_NOMINATIONS: ValidatorMetricSpec(
            ValidatorMetric.TOP_BY_NOMINATIONS, "top validators by nominations"
        ),
        ValidatorMetric.TOP_BY_DOMINANCE: ValidatorMetricSpec(
            ValidatorMetric.TOP_BY_DOMINANCE, "top validators by dominance"
        ),
    }

    def __init__(self, allowed_metrics: List[ValidatorMetric] = None):
        super().__init__("validator_metric", VariableType.TEXT)
        self.allowed_metrics = allowed_metrics or list(self.METRICS.keys())

    def sample(self, rng: random.Random) -> ValidatorMetricSpec:
        metric_type = rng.choice(self.allowed_metrics)
        return self.METRICS[metric_type]

    def get_display_value(self, value: ValidatorMetricSpec) -> str:
        return value.display_name

    def get_api_value(self, value: ValidatorMetricSpec) -> str:
        return value.metric.value


@register_template("taostats_validator")
class ValidatorTemplate(QuestionTemplate):
    """
    Template for querying Bittensor validator data on Taostats.

    Generates questions about validators including stake, nominations, etc.
    All data is available on https://taostats.io/validators
    """

    PATTERNS: Dict[ValidatorMetric, List[str]] = {
        ValidatorMetric.TOP_BY_STAKE: [
            "Go to taostats.io/validators and find the validator with the highest root stake.",
            "On taostats.io/validators, which validator has the most root stake?",
            "Visit taostats.io/validators and report the top validator by root stake.",
        ],
        ValidatorMetric.TOP_BY_NOMINATIONS: [
            "Go to taostats.io/validators and find which validator has the most nominations.",
            "On taostats.io/validators, which validator has the highest number of nominations?",
        ],
        ValidatorMetric.TOP_BY_DOMINANCE: [
            "Go to taostats.io/validators and find which validator has the highest dominance.",
            "On taostats.io/validators, which validator has the most dominance?",
        ],
    }

    def __init__(self):
        super().__init__("taostats_validator")
        self.register_variable(ValidatorMetricVariable())

    def generate(self, seed: int) -> GeneratedQuestion:
        rng = random.Random(seed)

        metric: ValidatorMetricSpec = self._variables["validator_metric"].sample(rng)

        patterns = self.PATTERNS.get(metric.metric, ["What is the {metric} on taostats?"])
        pattern = rng.choice(patterns)
        question_text = pattern.format(metric=metric.display_name)

        validation_info = {
            "metric": metric.metric.value,
            "display_name": metric.display_name,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://taostats.io/validators",
            variables={"metric": metric},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        metric = validation_info.get("metric", "")

        if metric == "top_by_stake":
            return """Task-Specific Rules (Top Validator by Stake):
- Score 1.0: Agent provides validator name with stake amount (numeric value)
- Score 0.5: Agent provides validator name only without stake amount
- Score 0.0: No answer, error message, or invalid format"""

        if metric == "top_by_nominations":
            return """Task-Specific Rules (Top Validator by Nominations):
- Score 1.0: Agent provides validator name with nomination count (numeric value)
- Score 0.5: Agent provides validator name only without count
- Score 0.0: No answer, error message, or invalid format"""

        if metric == "top_by_dominance":
            return """Task-Specific Rules (Top Validator by Dominance):
- Score 1.0: Agent provides validator name with dominance percentage
- Score 0.5: Agent provides validator name only without percentage
- Score 0.0: No answer, error message, or invalid format"""

        return """Task-Specific Rules:
- Score 1.0: Specific, well-formatted answer
- Score 0.5: Partially correct or approximate
- Score 0.0: No answer or clearly wrong"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[Any]:
        """
        Ground truth for validators is dynamic and best validated by LLM.
        Return None to trigger LLM validation.
        """
        return None

    async def validate_answer(
        self, answer: str, validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate answer - delegates to LLM validation"""
        return ValidationResult(
            score=0.0,
            is_correct=False,
            expected=None,
            actual=answer,
            details="Validator validation requires LLM",
        )
