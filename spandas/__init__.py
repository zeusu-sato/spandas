# spandas/__init__.py

"""
Spandas Package Initialization

This module initializes the spandas package, setting up all core imports and exposing
the enhanced Spandas class with extended pandas-like functionality on top of Spark.

To remain lightweight for environments without Spark, no ``pyspark`` imports are
performed at module import time.  Functions that require Spark will attempt to
import ``pyspark`` lazily and raise a clear error if it is unavailable.
"""

from importlib.metadata import PackageNotFoundError, version

try:  # pragma: no cover - version retrieval
    __version__ = version("spandas")
except PackageNotFoundError:  # pragma: no cover - fallback when not installed
    __version__ = "0.0.0"

from .spandas import Spandas, SpandasSeries

__all__ = ["Spandas", "SpandasSeries", "__version__"]
