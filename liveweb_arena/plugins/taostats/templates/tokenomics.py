"""Tokenomics query template for Taostats"""

import random
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
    Variable, VariableType
)


class TokenomicsMetric(Enum):
    """Metrics available on taostats.io/tokenomics - only those clearly visible"""
    CIRCULATING_SUPPLY = "circulating_supply"  # Top of page
    TOTAL_SUPPLY = "total_supply"  # Top of page
    NEXT_HALVING = "next_halving"  # Halving section
    IN_CIRCULATION_PCT = "in_circulation_pct"  # Percentage shown


@dataclass
class TokenomicsMetricSpec:
    """Specification for a tokenomics metric"""
    metric: TokenomicsMetric
    display_name: str
    unit: str = ""


class TokenomicsMetricVariable(Variable):
    """Variable for tokenomics metric selection"""

    METRICS: Dict[TokenomicsMetric, TokenomicsMetricSpec] = {
        TokenomicsMetric.CIRCULATING_SUPPLY: TokenomicsMetricSpec(
            TokenomicsMetric.CIRCULATING_SUPPLY, "circulating supply", "TAO"
        ),
        TokenomicsMetric.TOTAL_SUPPLY: TokenomicsMetricSpec(
            TokenomicsMetric.TOTAL_SUPPLY, "total supply", "TAO"
        ),
        TokenomicsMetric.NEXT_HALVING: TokenomicsMetricSpec(
            TokenomicsMetric.NEXT_HALVING, "next halving date", ""
        ),
        TokenomicsMetric.IN_CIRCULATION_PCT: TokenomicsMetricSpec(
            TokenomicsMetric.IN_CIRCULATION_PCT, "in circulation percentage", "%"
        ),
    }

    def __init__(self, allowed_metrics: List[TokenomicsMetric] = None):
        super().__init__("tokenomics_metric", VariableType.TEXT)
        self.allowed_metrics = allowed_metrics or list(self.METRICS.keys())

    def sample(self, rng: random.Random) -> TokenomicsMetricSpec:
        metric_type = rng.choice(self.allowed_metrics)
        return self.METRICS[metric_type]

    def get_display_value(self, value: TokenomicsMetricSpec) -> str:
        return value.display_name

    def get_api_value(self, value: TokenomicsMetricSpec) -> str:
        return value.metric.value


@register_template("taostats_tokenomics")
class TokenomicsTemplate(QuestionTemplate):
    """
    Template for querying TAO tokenomics data on Taostats.

    Generates questions about TAO supply, price, market cap, halving schedule, etc.
    All data is available on https://taostats.io/tokenomics
    """

    PATTERNS: Dict[TokenomicsMetric, List[str]] = {
        TokenomicsMetric.CIRCULATING_SUPPLY: [
            "Go to taostats.io/tokenomics and find the current circulating supply of TAO.",
            "On the taostats.io/tokenomics page, what is the circulating supply?",
            "Visit taostats.io/tokenomics and report the TAO circulating supply.",
        ],
        TokenomicsMetric.TOTAL_SUPPLY: [
            "Go to taostats.io/tokenomics and find the total supply of TAO.",
            "On the taostats.io/tokenomics page, what is TAO's total/max supply?",
        ],
        TokenomicsMetric.NEXT_HALVING: [
            "Go to taostats.io/tokenomics and find when the next TAO halving is.",
            "On the taostats.io/tokenomics page, what date is the next halving?",
        ],
        TokenomicsMetric.IN_CIRCULATION_PCT: [
            "Go to taostats.io/tokenomics and find what percentage of TAO is in circulation.",
            "On the taostats.io/tokenomics page, what is the 'In Circulation' percentage?",
        ],
    }

    def __init__(self):
        super().__init__("taostats_tokenomics")
        self.register_variable(TokenomicsMetricVariable())

    def generate(self, seed: int) -> GeneratedQuestion:
        rng = random.Random(seed)

        metric: TokenomicsMetricSpec = self._variables["tokenomics_metric"].sample(rng)

        patterns = self.PATTERNS.get(metric.metric, ["What is the {metric} on taostats?"])
        pattern = rng.choice(patterns)
        question_text = pattern.format(metric=metric.display_name)

        validation_info = {
            "metric": metric.metric.value,
            "display_name": metric.display_name,
            "unit": metric.unit,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://taostats.io/tokenomics",
            variables={"metric": metric},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        metric = validation_info.get("metric", "")

        if metric == "next_halving":
            return """Task-Specific Rules (Next Halving Date):
- Score 1.0: Agent provides a specific date (e.g., "December 12, 2029", "12 Dec 2029")
- Score 0.5: Agent provides approximate timeframe (e.g., "late 2029", "in about 4 years")
- Score 0.0: No date provided or clearly wrong"""

        if metric in ["circulating_supply", "total_supply"]:
            return """Task-Specific Rules (Supply Amount):
- Score 1.0: Agent provides a specific number (e.g., "10,591,079", "21,000,000", "10.5M")
- Score 0.5: Agent provides approximate value or different format
- Score 0.0: No value or clearly implausible number"""

        if metric == "in_circulation_pct":
            return """Task-Specific Rules (Percentage):
- Score 1.0: Agent provides a specific percentage (e.g., "50.43%", "50.4%")
- Score 0.5: Agent provides approximate percentage
- Score 0.0: No percentage or clearly wrong"""

        return """Task-Specific Rules:
- Score 1.0: Specific, well-formatted answer
- Score 0.5: Partially correct or approximate
- Score 0.0: No answer or clearly wrong"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[Any]:
        """
        Ground truth for tokenomics is dynamic and best validated by LLM.
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
            details="Tokenomics validation requires LLM",
        )
