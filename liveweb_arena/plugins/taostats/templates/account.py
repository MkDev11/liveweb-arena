"""Account query template for Taostats"""

import random
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
    Variable, VariableType
)


class AccountMetric(Enum):
    """Metrics available for account queries"""
    TOP_BY_BALANCE = "top_by_balance"
    ACCOUNT_COUNT = "account_count"


@dataclass
class AccountMetricSpec:
    """Specification for an account metric"""
    metric: AccountMetric
    display_name: str


class AccountMetricVariable(Variable):
    """Variable for account metric selection"""

    METRICS: Dict[AccountMetric, AccountMetricSpec] = {
        AccountMetric.TOP_BY_BALANCE: AccountMetricSpec(
            AccountMetric.TOP_BY_BALANCE, "top account by balance"
        ),
        AccountMetric.ACCOUNT_COUNT: AccountMetricSpec(
            AccountMetric.ACCOUNT_COUNT, "total account count"
        ),
    }

    def __init__(self):
        super().__init__("account_metric", VariableType.TEXT)

    def sample(self, rng: random.Random) -> AccountMetricSpec:
        metric = rng.choice(list(self.METRICS.keys()))
        return self.METRICS[metric]

    def get_display_value(self, value: AccountMetricSpec) -> str:
        return value.display_name

    def get_api_value(self, value: AccountMetricSpec) -> str:
        return value.metric.value


@register_template("taostats_account")
class AccountTemplate(QuestionTemplate):
    """
    Template for account queries on Taostats.

    Generates questions about Bittensor accounts/addresses.
    Data available on https://taostats.io/accounts
    """

    PATTERNS: Dict[AccountMetric, List[str]] = {
        AccountMetric.TOP_BY_BALANCE: [
            "Go to taostats.io/accounts and find which account has the highest free balance.",
            "On taostats.io/accounts, which address has the most TAO balance?",
        ],
        AccountMetric.ACCOUNT_COUNT: [
            "Go to taostats.io/accounts and find the total number of accounts on Bittensor.",
            "On taostats.io/accounts, how many total accounts exist on the network?",
        ],
    }

    def __init__(self):
        super().__init__("taostats_account")
        self.register_variable(AccountMetricVariable())

    def generate(self, seed: int) -> GeneratedQuestion:
        rng = random.Random(seed)

        metric: AccountMetricSpec = self._variables["account_metric"].sample(rng)

        patterns = self.PATTERNS.get(metric.metric, [])
        pattern = rng.choice(patterns)
        question_text = pattern

        validation_info = {
            "metric": metric.metric.value,
            "display_name": metric.display_name,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url="https://taostats.io/accounts",
            variables={"metric": metric},
            validation_info=validation_info,
            template_name=self.name,
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        metric = validation_info.get("metric", "")

        if metric == "top_by_balance":
            return """Task-Specific Rules (Top Account by Balance):
- Score 1.0: Agent provides account address/name with balance amount
- Score 0.5: Agent provides account address only without balance
- Score 0.0: No answer, error message, or invalid format"""

        if metric == "account_count":
            return """Task-Specific Rules (Account Count):
- Score 1.0: Agent provides a specific number (e.g., "420,589 accounts")
- Score 0.5: Agent provides approximate count
- Score 0.0: No count or clearly implausible number"""

        return """Task-Specific Rules:
- Score 1.0: Specific, well-formatted answer with concrete data
- Score 0.5: Partially complete answer
- Score 0.0: No answer or invalid format"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[Any]:
        """Account data is dynamic, use LLM validation"""
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
            details="Account validation requires LLM",
        )
