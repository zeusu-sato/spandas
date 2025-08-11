# spandas/__init__.py

"""
Spandas Package Initialization

This module initializes the spandas package, setting up all core imports and exposing
the enhanced Spandas class with extended pandas-like functionality on top of Spark.
"""

try:
    import pyspark  # noqa: F401
except Exception as e:  # pragma: no cover - simple import guard
    raise RuntimeError(
        "pyspark が見つかりません。Databricks では同梱されています。"
        "ローカルで使う場合は `pip install pyspark` か `pip install spandas[local]` を実行してください。"
    ) from e

from importlib.metadata import PackageNotFoundError, version

try:  # pragma: no cover - version retrieval
    __version__ = version("spandas")
except PackageNotFoundError:  # pragma: no cover - fallback when not installed
    __version__ = "0.0.0"

from .spandas import Spandas, SpandasSeries

__all__ = ["Spandas", "SpandasSeries", "__version__"]
