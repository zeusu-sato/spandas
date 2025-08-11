# spandas/__init__.py

"""
Spandas Package Initialization

This module initializes the spandas package, setting up all core imports and exposing
the enhanced Spandas class with extended pandas-like functionality on top of Spark.
"""

from .spandas import Spandas, SpandasSeries

__all__ = ["Spandas", "SpandasSeries"]
