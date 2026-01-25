"""TMDB question templates"""

# DISABLED templates (high memorization risk - models answer from training data):
# - TMDBMovieInfoTemplate: Basic movie facts (release date, runtime, language, director)
# - TMDBMovieCastTemplate: Lead actors of famous movies are well-known
# - TMDBMovieComparisonTemplate: Release order and runtime of famous movies
# These are still importable for backwards compatibility but not registered.
from .movie_info import TMDBMovieInfoTemplate
from .movie_cast import TMDBMovieCastTemplate
from .movie_comparison import TMDBMovieComparisonTemplate

# Active templates with lower memorization risk
from .movie_crew import TMDBMovieCrewTemplate  # Cinematographers, editors, etc. - less famous

# Anti-memorization templates (recommended for evaluation)
from .person_filmography import TMDBPersonFilmographyTemplate
from .movie_collection import TMDBMovieCollectionTemplate
from .aggregate import TMDBAggregateTemplate
from .cast_position import TMDBCastPositionTemplate  # "5th billed actor" - hard to memorize
from .recent_works import TMDBRecentWorksTemplate    # Dynamic data

__all__ = [
    # Disabled (importable but not registered)
    "TMDBMovieInfoTemplate",
    "TMDBMovieCastTemplate",
    "TMDBMovieComparisonTemplate",
    # Active (lower risk)
    "TMDBMovieCrewTemplate",
    # Recommended (anti-memorization)
    "TMDBPersonFilmographyTemplate",
    "TMDBMovieCollectionTemplate",
    "TMDBAggregateTemplate",
    "TMDBCastPositionTemplate",
    "TMDBRecentWorksTemplate",
]
