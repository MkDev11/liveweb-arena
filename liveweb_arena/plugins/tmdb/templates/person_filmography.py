"""Person filmography template for TMDB - HARD/MULTI-STEP DIFFICULTY"""

import random
from typing import Any, Dict, List, Optional

from liveweb_arena.core.validators.base import (
    QuestionTemplate, GeneratedQuestion, ValidationResult, register_template,
)
from liveweb_arena.core.ground_truth_trigger import (
    UrlPatternTrigger, FetchStrategy, TriggerConfig
)
from ..api_client import TMDBClient


class PersonSpec:
    """Specification of a person (actor/director)"""
    def __init__(self, person_id: str, name: str, known_for: str):
        self.person_id = person_id
        self.name = name
        self.known_for = known_for  # "acting" or "directing"


class PersonVariable:
    """Variable for person selection - directors and actors"""

    # Mix of well-known directors and actors with stable filmographies
    PERSONS: List[PersonSpec] = [
        # Directors
        PersonSpec("525", "Christopher Nolan", "directing"),
        PersonSpec("138", "Quentin Tarantino", "directing"),
        PersonSpec("1032", "Martin Scorsese", "directing"),
        PersonSpec("5655", "Ridley Scott", "directing"),
        PersonSpec("108", "Peter Jackson", "directing"),
        PersonSpec("5281", "Spike Lee", "directing"),
        PersonSpec("1884", "Denis Villeneuve", "directing"),
        PersonSpec("578", "Wes Anderson", "directing"),
        PersonSpec("5174", "Guillermo del Toro", "directing"),
        PersonSpec("7467", "David Fincher", "directing"),
        PersonSpec("488", "Steven Spielberg", "directing"),
        PersonSpec("24", "Clint Eastwood", "directing"),
        PersonSpec("1769", "Sofia Coppola", "directing"),
        PersonSpec("4762", "Michael Bay", "directing"),
        PersonSpec("11614", "James Wan", "directing"),
        # Actors with substantial filmographies
        PersonSpec("31", "Tom Hanks", "acting"),
        PersonSpec("6193", "Leonardo DiCaprio", "acting"),
        PersonSpec("1892", "Matt Damon", "acting"),
        PersonSpec("500", "Tom Cruise", "acting"),
        PersonSpec("192", "Morgan Freeman", "acting"),
        PersonSpec("1461", "George Clooney", "acting"),
        PersonSpec("2524", "Robert Downey Jr.", "acting"),
        PersonSpec("73968", "Henry Cavill", "acting"),
        PersonSpec("17419", "Bryan Cranston", "acting"),
        PersonSpec("2888", "Will Smith", "acting"),
        PersonSpec("3223", "Robert De Niro", "acting"),
        PersonSpec("1158", "Al Pacino", "acting"),
        PersonSpec("4785", "Samuel L. Jackson", "acting"),
        PersonSpec("17052", "Scarlett Johansson", "acting"),
        PersonSpec("1245", "Scarlett Johansson", "acting"),
    ]

    def __init__(self, role_filter: str = None):
        """
        Args:
            role_filter: "directing" or "acting" to filter persons, None for all
        """
        if role_filter:
            self.persons = [p for p in self.PERSONS if p.known_for == role_filter]
        else:
            self.persons = self.PERSONS

    def sample(self, rng: random.Random) -> PersonSpec:
        """Sample a person."""
        return rng.choice(self.persons)


class FilmographyQueryType:
    """Types of filmography queries"""
    MOVIE_COUNT = "movie_count"
    FIRST_MOVIE = "first_movie"
    LATEST_MOVIE = "latest_movie"
    SPECIFIC_YEAR_COUNT = "specific_year_count"


@register_template("tmdb_person_filmography")
class TMDBPersonFilmographyTemplate(QuestionTemplate):
    """
    Template for person filmography queries - HARD/MULTI-STEP DIFFICULTY.

    Requires:
    1. Navigate to TMDB
    2. Search for the person
    3. Go to their profile page
    4. Navigate to filmography section
    5. Find/count/analyze their work

    Examples:
    - How many movies has Christopher Nolan directed according to TMDB?
    - What was Quentin Tarantino's first movie as director?
    - What is the most recent movie Tom Hanks appeared in?
    - How many movies did Leonardo DiCaprio appear in during the 2010s?
    """

    DIRECTOR_COUNT_PATTERNS = [
        "How many movies has {person} directed according to TMDB?",
        "How many films has {person} directed?",
        "What is the total number of movies directed by {person} on TMDB?",
    ]

    ACTOR_COUNT_PATTERNS = [
        "How many movies has {person} appeared in according to TMDB?",
        "In how many films has {person} acted?",
        "What is the total number of movie credits for {person} as an actor on TMDB?",
    ]

    DIRECTOR_FIRST_PATTERNS = [
        "What was {person}'s first movie as director?",
        "What is the first film {person} directed?",
        "Which movie did {person} direct first?",
    ]

    ACTOR_FIRST_PATTERNS = [
        "What was {person}'s first movie?",
        "What is the first film {person} appeared in?",
        "Which movie marked {person}'s film debut?",
    ]

    DIRECTOR_LATEST_PATTERNS = [
        "What is the most recent movie directed by {person}?",
        "What is {person}'s latest film as director?",
        "Which movie did {person} most recently direct?",
    ]

    ACTOR_LATEST_PATTERNS = [
        "What is the most recent movie {person} appeared in?",
        "What is {person}'s latest film?",
        "Which movie did {person} most recently star in?",
    ]

    QUERY_TYPES = [
        FilmographyQueryType.MOVIE_COUNT,
        FilmographyQueryType.FIRST_MOVIE,
        FilmographyQueryType.LATEST_MOVIE,
    ]

    def __init__(self):
        super().__init__("tmdb_person_filmography")
        self._director_var = PersonVariable(role_filter="directing")
        self._actor_var = PersonVariable(role_filter="acting")

    def generate(self, seed: int, variant: Optional[int] = None) -> GeneratedQuestion:
        """Generate a person filmography question."""
        rng = random.Random(seed)

        # Select query type
        if variant is not None:
            query_type = self.QUERY_TYPES[variant % len(self.QUERY_TYPES)]
        else:
            query_type = rng.choice(self.QUERY_TYPES)

        # Randomly choose director or actor
        is_director = rng.choice([True, False])
        if is_director:
            person = self._director_var.sample(rng)
        else:
            person = self._actor_var.sample(rng)

        question_text = self._build_question(person, query_type, is_director, rng)

        # Start at TMDB home - agent must search for the person
        start_url = "https://www.themoviedb.org/"

        validation_info = {
            "person_id": person.person_id,
            "person_name": person.name,
            "is_director": is_director,
            "query_type": query_type,
        }

        return GeneratedQuestion(
            question_text=question_text,
            start_url=start_url,
            variables={"person": person, "query_type": query_type},
            validation_info=validation_info,
            template_name=self.name,
            expected_steps=20,  # Multi-step task needs more steps
        )

    def _build_question(
        self,
        person: PersonSpec,
        query_type: str,
        is_director: bool,
        rng: random.Random,
    ) -> str:
        """Build question text."""
        if query_type == FilmographyQueryType.MOVIE_COUNT:
            patterns = self.DIRECTOR_COUNT_PATTERNS if is_director else self.ACTOR_COUNT_PATTERNS
        elif query_type == FilmographyQueryType.FIRST_MOVIE:
            patterns = self.DIRECTOR_FIRST_PATTERNS if is_director else self.ACTOR_FIRST_PATTERNS
        else:  # LATEST_MOVIE
            patterns = self.DIRECTOR_LATEST_PATTERNS if is_director else self.ACTOR_LATEST_PATTERNS

        pattern = rng.choice(patterns)
        return pattern.format(person=person.name)

    def get_validation_rules(self, validation_info: Dict[str, Any]) -> str:
        """Get validation rules based on query type."""
        query_type = validation_info.get("query_type", FilmographyQueryType.MOVIE_COUNT)
        person_name = validation_info.get("person_name", "the person")
        is_director = validation_info.get("is_director", False)
        role = "director" if is_director else "actor"

        if query_type == FilmographyQueryType.MOVIE_COUNT:
            return f"""Task-Specific Rules (TMDB - Filmography Count):
- Count the number of movies {person_name} has as {role} credits on TMDB
- Score 1.0: Exact count match OR within 2 movies (filmographies update)
- Score 0.5: Within 5 movies of expected count
- Score 0.0: Count differs by more than 5
- Only count movies, not TV shows or other credits"""

        if query_type == FilmographyQueryType.FIRST_MOVIE:
            return f"""Task-Specific Rules (TMDB - First Movie):
- Find the earliest movie {person_name} worked on as {role}
- Score 1.0: Correct movie title (case insensitive, partial match OK)
- Score 0.0: Wrong movie
- The "first" movie is determined by release date, not credit order"""

        return f"""Task-Specific Rules (TMDB - Latest Movie):
- Find the most recent released movie {person_name} worked on as {role}
- Score 1.0: Correct movie title (case insensitive, partial match OK)
- Score 0.5: A recent movie but not the absolute latest
- Score 0.0: Wrong movie or old movie
- Only count released movies, not upcoming ones"""

    async def get_ground_truth(self, validation_info: Dict[str, Any]) -> Optional[str]:
        """Fetch person filmography from TMDB API."""
        person_id = validation_info.get("person_id", "")
        query_type = validation_info.get("query_type", FilmographyQueryType.MOVIE_COUNT)
        is_director = validation_info.get("is_director", False)

        if not person_id:
            return None

        try:
            # Get person's movie credits
            data = await TMDBClient.get(f"/person/{person_id}/movie_credits")
            if not data:
                return None

            if is_director:
                # Filter crew for directing jobs
                crew = data.get("crew", [])
                movies = [
                    m for m in crew
                    if m.get("job") == "Director" and m.get("release_date")
                ]
            else:
                # Use cast credits
                movies = [
                    m for m in data.get("cast", [])
                    if m.get("release_date")
                ]

            if not movies:
                return None

            # Sort by release date
            movies_sorted = sorted(
                movies,
                key=lambda m: m.get("release_date", ""),
            )

            # Filter out future releases for "latest" query
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")

            if query_type == FilmographyQueryType.MOVIE_COUNT:
                # Count unique movies (some may have multiple credits)
                unique_ids = set(m.get("id") for m in movies)
                return str(len(unique_ids))

            elif query_type == FilmographyQueryType.FIRST_MOVIE:
                # Get first movie by release date
                first = movies_sorted[0]
                return f"{first.get('title')} ({first.get('release_date', '')[:4]})"

            else:  # LATEST_MOVIE
                # Get latest released movie (not future)
                released = [m for m in movies_sorted if m.get("release_date", "") <= today]
                if released:
                    latest = released[-1]
                    return f"{latest.get('title')} ({latest.get('release_date', '')[:4]})"
                return None

        except Exception:
            return None

    async def validate_answer(
        self,
        answer: str,
        validation_info: Dict[str, Any]
    ) -> ValidationResult:
        """Validate filmography answer."""
        ground_truth = await self.get_ground_truth(validation_info)

        if ground_truth is None:
            return ValidationResult(
                score=0.0,
                is_correct=False,
                expected=None,
                actual=answer,
                details="Ground truth unavailable",
            )

        query_type = validation_info.get("query_type", FilmographyQueryType.MOVIE_COUNT)

        if query_type == FilmographyQueryType.MOVIE_COUNT:
            return self._validate_count(answer, ground_truth)
        else:
            return self._validate_movie_title(answer, ground_truth)

    def _validate_count(self, answer: str, expected: str) -> ValidationResult:
        """Validate movie count answer."""
        import re

        # Parse expected count
        try:
            exp_count = int(expected)
        except ValueError:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not parse expected count",
            )

        # Parse answer - find first number
        num_match = re.search(r"\b(\d+)\b", answer)
        if not num_match:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details="Could not find a number in answer",
            )

        ans_count = int(num_match.group(1))
        diff = abs(ans_count - exp_count)

        if diff <= 2:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details=f"Count within tolerance (diff: {diff})",
            )
        elif diff <= 5:
            return ValidationResult(
                score=0.5, is_correct=False, expected=expected,
                actual=answer, details=f"Count close but outside tight tolerance (diff: {diff})",
            )
        else:
            return ValidationResult(
                score=0.0, is_correct=False, expected=expected,
                actual=answer, details=f"Count too far off (diff: {diff})",
            )

    def _validate_movie_title(self, answer: str, expected: str) -> ValidationResult:
        """Validate movie title answer."""
        import re

        answer_lower = answer.lower().strip()

        # Expected format: "Movie Title (YYYY)"
        match = re.match(r"(.+?)\s*\((\d{4})\)", expected)
        if match:
            exp_title = match.group(1).lower()
            exp_year = match.group(2)
        else:
            exp_title = expected.lower()
            exp_year = None

        # Check if title appears in answer
        if exp_title in answer_lower:
            return ValidationResult(
                score=1.0, is_correct=True, expected=expected,
                actual=answer, details="Movie title matches",
            )

        # Check for partial title match (for long titles)
        title_words = exp_title.split()
        if len(title_words) >= 3:
            # Check if most words appear
            matches = sum(1 for w in title_words if w in answer_lower and len(w) > 2)
            if matches >= len(title_words) * 0.7:
                return ValidationResult(
                    score=1.0, is_correct=True, expected=expected,
                    actual=answer, details="Most title words match",
                )

        # Check if year is mentioned (partial credit)
        if exp_year and exp_year in answer:
            return ValidationResult(
                score=0.3, is_correct=False, expected=expected,
                actual=answer, details="Year matches but title doesn't",
            )

        return ValidationResult(
            score=0.0, is_correct=False, expected=expected,
            actual=answer, details="Movie title not found in answer",
        )

    def get_ground_truth_trigger(
        self,
        validation_info: Dict[str, Any]
    ) -> TriggerConfig:
        """Trigger when agent visits the person's TMDB page."""
        person_id = validation_info.get("person_id", "")
        trigger = UrlPatternTrigger(
            domains=["themoviedb.org"],
            url_contains=f"/person/{person_id}" if person_id else None,
        )
        return TriggerConfig(trigger=trigger, strategy=FetchStrategy.FIRST)
