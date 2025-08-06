# spandas/enhanced/aggregation/aggregation.py

"""
spandas.enhanced.aggregation: Implements aggregation methods such as agg, groupby, describe.
"""

__all__ = ["agg", "groupby", "describe"]

from typing import Union, Callable, List, Dict, Optional
import pandas as pd
import pyspark.pandas as ps


def agg(
    self: ps.DataFrame,
    func: Union[str, List[str], Dict[str, Union[str, List[str], Callable]]],
    axis: int = 0,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> Union[ps.DataFrame, ps.Series]:
    """
    Aggregate using one or more operations over the specified axis.

    Args:
        func (Union[str, list, dict]): Aggregation function(s).
        axis (int): Axis over which to apply.
        to_pandas (bool): Whether to use full pandas for accuracy.

    Returns:
        Union[ps.DataFrame, ps.Series]: Aggregated result.
    """
    if to_pandas:
        result = self.to_pandas().agg(func, axis=axis, *args, **kwargs)
        return ps.from_pandas(result)
    else:
        return self.agg(func, axis=axis, *args, **kwargs)


def groupby(
    self: ps.DataFrame,
    by: Union[str, List[str]],
    agg_func: Union[str, Callable, Dict[str, Union[str, List[str]]]] = "mean",
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Group DataFrame using a mapper or by a Series of columns.

    Args:
        by (Union[str, List[str]]): Column(s) to group by.
        agg_func (Union[str, Callable, Dict]): Aggregation method.
        to_pandas (bool): Whether to use pandas for full functionality.

    Returns:
        ps.DataFrame: Grouped and aggregated result.
    """
    if to_pandas:
        pdf = self.to_pandas()
        result = pdf.groupby(by, *args, **kwargs).agg(agg_func)
        return ps.from_pandas(result)
    else:
        return self.groupby(by, *args, **kwargs).agg(agg_func)


def describe(
    self: ps.DataFrame,
    percentiles: Optional[List[float]] = None,
    include: Optional[Union[str, List[str]]] = None,
    exclude: Optional[Union[str, List[str]]] = None,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Generate descriptive statistics.

    Args:
        percentiles (List[float], optional): List of percentiles to include.
        include (str | list, optional): Data types to include.
        exclude (str | list, optional): Data types to exclude.
        to_pandas (bool): Whether to use pandas describe.

    Returns:
        ps.DataFrame: Descriptive statistics.
    """
    if to_pandas:
        return ps.from_pandas(
            self.to_pandas().describe(percentiles=percentiles, include=include, exclude=exclude)
        )
    else:
        return self.describe()
