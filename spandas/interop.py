"""Minimal Spark/pandas interop helpers."""

from __future__ import annotations

from typing import Optional

import pandas as pd


def _require_pyspark():
    try:  # pragma: no cover - dynamic import
        import pyspark.sql  # noqa: F401
    except Exception as e:  # pragma: no cover - environment without pyspark
        raise RuntimeError("pyspark not available (Databricks-only).") from e


def to_pandas(sdf) -> pd.DataFrame:
    """Convert a :class:`pyspark.sql.DataFrame` to :class:`pandas.DataFrame`."""
    _require_pyspark()
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore

    if not isinstance(sdf, SparkDataFrame):
        raise TypeError("sdf must be a pyspark.sql.DataFrame")

    return sdf.toPandas()


def to_spark(pdf: pd.DataFrame, spark: Optional["SparkSession"] = None):
    """Convert a :class:`pandas.DataFrame` to :class:`pyspark.sql.DataFrame`."""
    _require_pyspark()
    from pyspark.sql import SparkSession

    if spark is None:
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    if not isinstance(pdf, pd.DataFrame):
        raise TypeError("pdf must be a pandas.DataFrame")

    return spark.createDataFrame(pdf)
