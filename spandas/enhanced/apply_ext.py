# spandas/enhanced/apply.py

"""
spandas.enhanced.apply: Enhanced versions of apply, applymap, and map using swifter (optional).
"""

import pandas as pd
from spandas.compat import ps
from typing import Callable, Any, Union, Optional

__all__ = [
    "apply",
    "applymap",
    "map",
    "transform",
    "pipe",
    "where",
    "mask",
    "combine",
    "combine_first",
]


def apply(
    self: ps.DataFrame,
    func: Callable,
    axis: int = 1,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> Union[ps.DataFrame, ps.Series]:
    """
    Apply a function along an axis of the DataFrame.
    If `to_pandas=True`, convert to pandas and use swifter for acceleration.

    Args:
        func (Callable): Function to apply to each row or column.
        axis (int): Axis to apply the function on (0 or 1).
        to_pandas (bool): Whether to use pandas and swifter for full compatibility.

    Returns:
        Union[ps.DataFrame, ps.Series]: Result of applying the function.
    """
    if to_pandas:
        pd_df = self.to_pandas()
        result = pd_df.swifter.apply(func, axis=axis, *args, **kwargs)
        return ps.from_pandas(result)
    else:
        if axis == 1:
            # Best-effort row-wise apply using Spark UDFs (approximation)
            from pyspark.sql.functions import pandas_udf, struct
            import inspect

            def _wrap(func):
                def inner(row):
                    return func(row.asDict())
                return inner

            udf_func = pandas_udf(_wrap(func))
            return self._internal.spark_frame.select(
                *[self[col]._expr for col in self.columns],
                udf_func(struct(*[self[col]._expr for col in self.columns])).alias("result")
            ).to_pandas()["result"]
        else:
            # Column-wise best-effort
            return self.map_partitions(lambda pdf: pdf.apply(func, axis=axis, *args, **kwargs))

def applymap(
    self: ps.DataFrame,
    func: Callable,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Apply a function to a DataFrame elementwise.
    If `to_pandas=True`, use swifter for pandas compatibility.

    Args:
        func (Callable): Elementwise function to apply.
        to_pandas (bool): Whether to use pandas + swifter for full compatibility.

    Returns:
        ps.DataFrame: Transformed DataFrame.
    """
    if to_pandas:
        pd_df = self.to_pandas()
        result = pd_df.swifter.applymap(func, *args, **kwargs)
        return ps.from_pandas(result)
    else:
        for col in self.columns:
            self[col] = self[col].map(func)
        return self

def map(
    self: ps.Series,
    func: Callable,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.Series:
    """
    Map values of Series using input function.

    Args:
        func (Callable): Function to apply to each element.
        to_pandas (bool): Whether to convert to pandas for compatibility.

    Returns:
        ps.Series: Transformed Series.
    """
    if to_pandas:
        pd_series = self.to_pandas()
        result = pd_series.swifter.map(func, *args, **kwargs)
        return ps.from_pandas(result)
    else:
        return self.map(func, *args, **kwargs)

def transform(
    self: ps.DataFrame,
    func: Callable,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Return a DataFrame with transformed values.

    Args:
        func (Callable): Function to use for transforming the data.
        to_pandas (bool): Use pandas backend with swifter if True.

    Returns:
        ps.DataFrame: Transformed DataFrame.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().swifter.transform(func, *args, **kwargs))
    else:
        return self.map_partitions(lambda pdf: pdf.transform(func, *args, **kwargs))

def pipe(
    self: ps.DataFrame,
    func: Union[Callable, str],
    *args,
    to_pandas: bool = False,
    **kwargs
) -> ps.DataFrame:
    """
    Apply function to DataFrame, useful for chaining.

    Args:
        func (Callable or str): Function to apply.
        to_pandas (bool): Use pandas backend if True.

    Returns:
        ps.DataFrame: Result after applying function.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().pipe(func, *args, **kwargs))
    else:
        return func(self, *args, **kwargs)

def where(
    self: ps.DataFrame,
    cond: Union[ps.DataFrame, pd.DataFrame],
    other: Any,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Replace values where the condition is False.

    Args:
        cond (DataFrame): Condition DataFrame.
        other (Any): Replacement value.
        to_pandas (bool): Use pandas backend if True.

    Returns:
        ps.DataFrame: Modified DataFrame.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().where(cond.to_pandas(), other))
    else:
        return self.mask(~cond, other)

def mask(
    self: ps.DataFrame,
    cond: Union[ps.DataFrame, pd.DataFrame],
    other: Any,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Replace values where the condition is True.

    Args:
        cond (DataFrame): Condition DataFrame.
        other (Any): Replacement value.
        to_pandas (bool): Use pandas backend if True.

    Returns:
        ps.DataFrame: Modified DataFrame.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().mask(cond.to_pandas(), other))
    else:
        return self.combine(cond, lambda x, y: y if y else x if not y else other)

def combine(
    self: ps.DataFrame,
    other: ps.DataFrame,
    func: Callable,
    fill_value: Optional[Any] = None,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Combine two DataFrames using a function.

    Args:
        other (DataFrame): Second DataFrame.
        func (Callable): Function to apply.
        fill_value (Optional[Any]): Value to use when an entry is missing.
        to_pandas (bool): Use pandas backend if True.

    Returns:
        ps.DataFrame: Combined DataFrame.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().combine(other.to_pandas(), func, fill_value))
    else:
        filled_self = self.fillna(fill_value) if fill_value is not None else self
        filled_other = other.fillna(fill_value) if fill_value is not None else other
        return filled_self.map_partitions(lambda df: df.combine(filled_other.to_pandas(), func))

def combine_first(
    self: ps.DataFrame,
    other: ps.DataFrame,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Update null elements with value in the same location from another DataFrame.

    Args:
        other (DataFrame): Second DataFrame to use for filling.
        to_pandas (bool): Use pandas backend if True.

    Returns:
        ps.DataFrame: Resulting DataFrame.
    """
    if to_pandas:
        return ps.from_pandas(self.to_pandas().combine_first(other.to_pandas()))
    else:
        return self.fillna(other)
