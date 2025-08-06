# spandas/enhanced/missing.py

"""
spandas.enhanced.missing: Enhanced missing data handling methods.
"""

from typing import Any, Union
import pandas as pd
import pyspark.pandas as ps

__all__ = [
    "isna",
    "notna",
    "dropna",
    "fillna",
]

def isna(self: Union[ps.DataFrame, ps.Series]) -> Union[ps.DataFrame, ps.Series]:
    """
    Detect missing values for each element.

    Returns:
        DataFrame or Series of booleans.
    """
    return self.isna()

def notna(self: Union[ps.DataFrame, ps.Series]) -> Union[ps.DataFrame, ps.Series]:
    """
    Detect non-missing values for each element.

    Returns:
        DataFrame or Series of booleans.
    """
    return self.notna()

def dropna(
    self: ps.DataFrame,
    axis: int = 0,
    how: str = "any",
    thresh: Union[int, None] = None,
    subset: Union[str, list, None] = None
) -> ps.DataFrame:
    """
    Remove missing values.

    Args:
        axis (int): 0 to drop rows, 1 to drop columns.
        how (str): 'any' or 'all'.
        thresh (int, optional): Require that many non-NA values.
        subset (str or list, optional): Columns to consider.

    Returns:
        ps.DataFrame: Cleaned DataFrame.
    """
    return self.dropna(axis=axis, how=how, thresh=thresh, subset=subset)

def fillna(
    self: Union[ps.DataFrame, ps.Series],
    value: Any = None,
    method: Union[str, None] = None,
    axis: Union[None, int] = None,
    limit: Union[int, None] = None
) -> Union[ps.DataFrame, ps.Series]:
    """
    Fill missing values.

    Args:
        value (scalar, dict, Series, or DataFrame): Value to use for filling NA.
        method (str, optional): Method to use for filling ('ffill', 'bfill').
        axis (int, optional): Axis along which to fill.
        limit (int, optional): Maximum number of NaN values to fill.

    Returns:
        ps.DataFrame or ps.Series: Filled object.
    """
    return self.fillna(value=value, method=method, axis=axis, limit=limit)
