"""Comparison query template for Taostats"""

import random
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
    Variable, VariableType
)


class ComparisonType(Enum):
    """Types of comparisons available"""
    SUBNET_MARKET_CAP = "subnet_market_cap"
    SUBNET_EMISSION = "subnet_emission"
    SUBNET_PRICE = "subnet_price"


@dataclass
class ComparisonSpec:
    """Specification for a comparison question"""
    comparison_type: ComparisonType
    display_name: str
    metric_name: str


# Top-ranked subnets for comparisons (likely visible on first page of /subnets)
COMPARISON_SUBNETS = [
    (1, "Apex"),
    (19, "ItsAI"),
    (51, "lium.io"),
    (64, "Chutes"),
    (9, "Pretrain"),
]


class ComparisonTypeVariable(Variable):
    """Variable for comparison type selection"""

    TYPES: Dict[ComparisonType, ComparisonSpec] = {
        ComparisonType.SUBNET_MARKET_CAP: ComparisonSpec(
            ComparisonType.SUBNET_MARKET_CAP, "market cap comparison", "market cap"
        ),
        ComparisonType.SUBNET_EMISSION: ComparisonSpec(
            ComparisonType.SUBNET_EMISSION, "emission comparison", "emission rate"
        ),
        ComparisonType.SUBNET_PRICE: ComparisonSpec(
            ComparisonType.SUBNET_PRICE, "price comparison", "alpha price"
        ),
    }

    def __init__(self, allowed_types: List[ComparisonType] = None):
        super().__init__("comparison_type", VariableType.TEXT)
        self.allowed_types = allowed_types or list(self.TYPES.keys())

    def sample(self, rng: random.Random) -> ComparisonSpec:
        comp_type = rng.choice(self.allowed_types)
        return self.TYPES[comp_type]

    def get_display_value(self, value: ComparisonSpec) -> str:
        return value.display_name

    def get_api_value(self, value: ComparisonSpec) -> str:
        return value.comparison_type.value


@register_template("taostats_comparison")
class ComparisonTemplate(QuestionTemplate):
    """
    Template for comparison questions on Taostats.

    Generates questions that require comparing two subnets on a specific metric.
    Adds complexity by requiring the agent to read and compare multiple data points.
    """

    PATTERNS: Dict[ComparisonType, List[str]] = {
        ComparisonType.SUBNET_MARKET_CAP: [
            "On taostats.io/subnets, compare the market caps of {subnet1} and {subnet2}. Which one is higher?",
            "Go to taostats.io/subnets and check: does {subnet1} or {subnet2} have a higher market cap?",
        ],
        ComparisonType.SUBNET_EMISSION: [
            "On taostats.io/subnets, compare the emission rates of {subnet1} and {subnet2}. Which has higher emission?",
            "Go to taostats.io/subnets and check: which subnet has higher emission, {subnet1} or {subnet2}?",
        ],
        ComparisonType.SUBNET_PRICE: [
            "On taostats.io/subnets, compare the alpha prices of {subnet1} and {subnet2}. Which is more expensive?",
            "Go to taostats.io/subnets and check: does {subnet1} or {subnet2} have a higher alpha price?",
        ],
    }

    def __init__(self):
        super().__init__("taostats_comparison")
        self.register_variable(ComparisonTypeVariable())

    def _select_subnet_pair(self, rng: random.Random) -> Tuple[Tuple[int, str], Tuple[int, str]]:
        """Select two different subnets for comparison"""
        subnets = rng.sample(COMPARISON_SUBNETS, 2)
        return subnets[0], subnets[1]

    def generate(self, seed: int) -> GeneratedQuestion:
        rng = random.Random(seed)

        comp_spec: ComparisonSpec = self._variables["comparison_type"].sample(rng)
        subnet1, subnet2 = self._select_subnet_pair(rng)

        patterns = self.PATTERNS.get(comp_spec.comparison_type, [])
        pattern = rng.choice(patterns)
        question_text = pattern.format(
            subnet1=subnet1[1],
            subnet2=subnet2[1],
        )

        validation_info = {
            "comparison_type": comp_spec.comparison_type.value,
            "metric_name": comp_spec.metric_name,
            "subnet1_id": subnet1[0],
            "subnet1_name": subnet1[1],
            "subnet2_id": subnet2[0],
            "subnet2_name": subnet2[1],
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://taostats.io/subnets",
            variables={"comparison": comp_spec},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        metric_name = validation_info.get("metric_name", "metric")
        subnet1 = validation_info.get("subnet1_name", "subnet A")
        subnet2 = validation_info.get("subnet2_name", "subnet B")

        return f"""Task-Specific Rules (Comparison - {metric_name}):
- Score 1.0: Agent correctly identifies which subnet ({subnet1} or {subnet2}) has higher {metric_name}, with specific values
- Score 0.5: Agent identifies the correct subnet but without specific values, or values are approximate
- Score 0.0: Agent identifies wrong subnet, or provides no comparison"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[Any]:
        """Ground truth requires real-time data, use LLM validation"""
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
            details="Comparison validation requires LLM",
        )
