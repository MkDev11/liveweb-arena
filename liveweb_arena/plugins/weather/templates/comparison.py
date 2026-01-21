"""Weather comparison template - MULTI-STEP INTERACTION"""

import random
from typing import Any, Dict, List, Optional
import aiohttp

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from .variables import LocationVariable, LocationSpec


@register_template("weather_comparison")
class WeatherComparisonTemplate(QuestionTemplate):
    """
    Template for comparing weather between two cities - MULTI-STEP INTERACTION.

    Requires the agent to:
    1. Visit first city's weather page
    2. Visit second city's weather page
    3. Compare the temperatures

    Examples:
    - Which city is warmer right now, Tokyo or London?
    - Is it hotter in New York or Los Angeles today?
    - Compare the current temperature in Paris and Berlin.
    """

    COMPARISON_PATTERNS = [
        "Which city is warmer right now, {city1} or {city2}?",
        "Is it hotter in {city1} or {city2} at this moment?",
        "Compare the current temperature: {city1} vs {city2}. Which is warmer?",
        "Between {city1} and {city2}, which city has higher temperature right now?",
    ]

    def __init__(self):
        super().__init__("weather_comparison")
        self._location_var = LocationVariable()

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a weather comparison question."""
        rng = random.Random(seed)

        # Sample two different cities
        city1 = self._location_var.sample(rng)
        city2 = self._location_var.sample(rng)

        # Ensure they're different
        attempts = 0
        while city2.display_name == city1.display_name and attempts < 10:
            city2 = self._location_var.sample(rng)
            attempts += 1

        pattern = rng.choice(self.COMPARISON_PATTERNS)
        question_text = pattern.format(city1=city1.display_name, city2=city2.display_name)

        validation_info = {
            "city1_name": city1.display_name,
            "city1_query": city1.api_query,
            "city2_name": city2.display_name,
            "city2_query": city2.api_query,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=f"https://wttr.in/{city1.api_query}",
            variables={"city1": city1, "city2": city2},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=8,  # Need to visit two pages
        )

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        city1 = validation_info.get("city1_name", "City1")
        city2 = validation_info.get("city2_name", "City2")
        return f"""Task-Specific Rules (Weather Comparison):
- Answer must clearly state which city ({city1} or {city2}) is warmer
- Score 1.0: Correct city identified
- Score 0.0: Wrong city or unclear answer
- Accept: "{city1}", "{city1} is warmer", "It's hotter in {city1}", temperature values with comparison"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch temperatures for both cities from wttr.in API."""
        city1_query = validation_info.get("city1_query", "")
        city2_query = validation_info.get("city2_query", "")
        city1_name = validation_info.get("city1_name", "")
        city2_name = validation_info.get("city2_name", "")

        if not city1_query or not city2_query:
            return None

        try:
            async with aiohttp.ClientSession() as session:
                # Fetch city1 temperature
                url1 = f"https://wttr.in/{city1_query}?format=j1"
                async with session.get(url1, timeout=aiohttp.ClientTimeout(total=15)) as resp1:
                    if resp1.status != 200:
                        return None
                    data1 = await resp1.json()

                # Fetch city2 temperature
                url2 = f"https://wttr.in/{city2_query}?format=j1"
                async with session.get(url2, timeout=aiohttp.ClientTimeout(total=15)) as resp2:
                    if resp2.status != 200:
                        return None
                    data2 = await resp2.json()

            # Get current temperatures
            temp1 = int(data1.get("current_condition", [{}])[0].get("temp_C", 0))
            temp2 = int(data2.get("current_condition", [{}])[0].get("temp_C", 0))

            if temp1 > temp2:
                return f"{city1_name} ({temp1}°C vs {temp2}°C)"
            elif temp2 > temp1:
                return f"{city2_name} ({temp2}°C vs {temp1}°C)"
            else:
                return f"Same temperature ({temp1}°C)"

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate comparison answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        city1_name = validation_info.get("city1_name", "").lower()
        city2_name = validation_info.get("city2_name", "").lower()
        answer_lower = answer.lower()

        # Handle "same temperature" case
        if "same" in ground_truth.lower():
            if "same" in answer_lower or "equal" in answer_lower:
                return ValidationResult(
                    score=1.0,
                    is_correct=True,
                    expected=ground_truth,
                    actual=answer,
                    details="Correctly identified same temperature",
                )

        # Extract winner from ground truth
        winner = ground_truth.split(" (")[0].lower()

        # Check if answer mentions the correct city
        if winner in answer_lower:
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=ground_truth,
                actual=answer,
                details="Correct city identified",
            )

        # Check for partial matches (first word of city name)
        winner_parts = winner.split()
        if any(part in answer_lower for part in winner_parts if len(part) > 3):
            return ValidationResult(
                score=1.0,
                is_correct=True,
                expected=ground_truth,
                actual=answer,
                details="Correct city identified (partial match)",
            )

        return ValidationResult(
            score=0.0,
            is_correct=False,
            expected=ground_truth,
            actual=answer,
            details="Wrong city or unclear answer",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when AI visits the second city's page."""
        city2_query = validation_info.get("city2_query", "")
        trigger = UrlPatternTrigger(
            domains=["wttr.in", "v2.wttr.in"],
            url_contains=city2_query.replace("+", " ").split(",")[0] if city2_query else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
