"""Open Library plugin for browsing and querying book data."""

from .openlibrary import OpenLibraryPlugin

# Import templates to register them
from . import templates

__all__ = ["OpenLibraryPlugin"]
