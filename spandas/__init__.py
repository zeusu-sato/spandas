# spandas/__init__.py

"""
Spandas Package Initialization

This module initializes the spandas package, setting up all core imports and exposing
the enhanced Spandas class with extended pandas-like functionality on top of Spark.
"""

try:
    import pyspark  # noqa: F401
except ImportError as exc:  # pragma: no cover - simple import guard
    raise RuntimeError(
        "pyspark is required but not installed. Please install pyspark to use spandas."
    ) from exc

from .spandas import Spandas, SpandasSeries

__all__ = ["Spandas", "SpandasSeries"]
