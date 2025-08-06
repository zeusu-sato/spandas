# spandas/enhanced/selection/filter_mask.py

"""
spandas.enhanced.selection.filter_mask: Boolean filtering utilities (isin, where, mask).
"""

from typing import Any, Union, List, Callable
import pyspark.pandas as ps
import pandas as pd

__all__ = ["isin", "where", "mask"]

def isin(
    self: Union[ps.Series, ps.DataFrame],
    values: Union[List[Any], ps.Series, ps.DataFrame],
    to_pandas: bool = False
) -> Union[ps.Series, ps.DataFrame]:
    """
    Check whether each element is in the given values.

    Args:
        self (Series or DataFrame): Input Spark object.
        values (list, Series, or DataFrame): Values to check against.
        to_pandas (bool): Whether to use pandas for full compatibility.

    Returns:
        Series or DataFrame of booleans.
    """
    if to_pandas:
        return self.to_pandas().isin(values)
    else:
        return self.isin(values)


def where(
    self: Union[ps.Series, ps.DataFrame],
    cond: Union[ps.Series, pd.Series],
    other: Any = None,
    to_pandas: bool = False
) -> Union[ps.Series, ps.DataFrame]:
    """
    Replace values where the condition is False.

    Args:
        self (Series or DataFrame): Input Spark object.
        cond (Series): Boolean Series indicating where to keep values.
        other (scalar or Series): Value to replace where condition is False.
        to_pandas (bool): Whether to use pandas for full compatibility.

    Returns:
        Series or DataFrame: Result after applying condition.
    """
    if to_pandas:
        return self.to_pandas().where(cond.to_pandas() if isinstance(cond, ps.Series) else cond, other)
    else:
        return self.where(cond, other)


def mask(
    self: Union[ps.Series, ps.DataFrame],
    cond: Union[ps.Series, pd.Series],
    other: Any = None,
    to_pandas: bool = False
) -> Union[ps.Series, ps.DataFrame]:
    """
    Replace values where the condition is True.

    Args:
        self (Series or DataFrame): Input Spark object.
        cond (Series): Boolean Series indicating where to mask values.
        other (scalar or Series): Value to use for masking.
        to_pandas (bool): Whether to use pandas for full compatibility.

    Returns:
        Series or DataFrame: Masked result.
    """
    if to_pandas:
        return self.to_pandas().mask(cond.to_pandas() if isinstance(cond, ps.Series) else cond, other)
    else:
        return self.mask(cond, other)
