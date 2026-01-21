"""Weather-specific question templates"""

# Import templates to trigger registration via @register_template decorator
from .templates import LocationNameWeatherTemplate, CurrentWeatherTemplate, MultiDayWeatherTemplate
from .time_of_day import TimeOfDayWeatherTemplate
from .astronomy import AstronomyTemplate
from .comparison import WeatherComparisonTemplate

# Re-export registration utilities from core
from liveweb_arena.core.validators.base import register_template, get_registered_templates, get_template

from .variables import (
    LocationVariable, DateVariable, WeatherMetricVariable, TimeOfDayVariable,
    LocationType, DateType, MetricType, TimeOfDay,
    LocationSpec, DateSpec, MetricSpec, TimeOfDaySpec,
)

__all__ = [
    # Templates
    "LocationNameWeatherTemplate",
    "CurrentWeatherTemplate",
    "MultiDayWeatherTemplate",
    "TimeOfDayWeatherTemplate",
    "AstronomyTemplate",
    "WeatherComparisonTemplate",
    # Registration utilities
    "register_template",
    "get_registered_templates",
    "get_template",
    # Variables
    "LocationVariable",
    "DateVariable",
    "WeatherMetricVariable",
    "TimeOfDayVariable",
    "LocationType",
    "DateType",
    "MetricType",
    "TimeOfDay",
    "LocationSpec",
    "DateSpec",
    "MetricSpec",
    "TimeOfDaySpec",
]
