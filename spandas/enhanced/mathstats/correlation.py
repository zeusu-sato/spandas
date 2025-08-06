# spandas/enhanced/mathstats/correlation.py

"""
spandas.enhanced.mathstats.correlation: Implements correlation and covariance
methods for Spandas using Spark where possible, with pandas fallback.

Provides:
- corr(): Pearson correlation between columns
- cov(): Covariance between columns
"""

from typing import Optional
import pyspark.pandas as ps
import pandas as pd

def corr(
    self: ps.DataFrame,
    method: str = 'pearson',
    min_periods: Optional[int] = None,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Compute pairwise correlation of columns.

    Args:
        method (str): Correlation method - only 'pearson' supported in Spark.
        min_periods (Optional[int]): Minimum number of observations (used only in pandas).
        to_pandas (bool): If True, use pandas implementation for full compatibility.

    Returns:
        ps.DataFrame: Correlation matrix.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().corr(method=method, min_periods=min_periods))
    else:
        if method != "pearson":
            raise ValueError("Only 'pearson' method is supported without to_pandas=True.")
        return self.corr(method="pearson")

def cov(
    self: ps.DataFrame,
    min_periods: Optional[int] = None,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Compute pairwise covariance of columns.

    Args:
        min_periods (Optional[int]): Minimum number of observations (used only in pandas).
        to_pandas (bool): If True, use pandas implementation for full compatibility.

    Returns:
        ps.DataFrame: Covariance matrix.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().cov(min_periods=min_periods))
    else:
        return self.cov()
