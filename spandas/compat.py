"""Compatibility helpers for PySpark pandas API across versions."""

try:
    import pyspark.pandas as ps  # PySpark < 4.x
except ImportError:  # pragma: no cover - for PySpark >= 4.x
    from pyspark import pandas as ps  # type: ignore

__all__ = ["ps"]

