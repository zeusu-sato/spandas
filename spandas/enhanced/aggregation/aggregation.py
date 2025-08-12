# spandas/enhanced/aggregation/aggregation.py

"""
spandas.enhanced.aggregation: Implements aggregation methods such as agg, groupby, describe.
"""

__all__ = ["agg", "groupby", "describe"]

from typing import Union, Callable, List, Dict, Optional
import pandas as pd
from spandas.compat import ps


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


class _PandasGroupBy:
    """Simple wrapper around :class:`pandas.core.groupby.DataFrameGroupBy`.

    The wrapper converts results back into pandas-on-Spark via ``ps.from_pandas``
    so that chaining with ``Spandas`` continues to work.
    """

    def __init__(self, pdf_gb: pd.core.groupby.generic.DataFrameGroupBy):
        self._gb = pdf_gb

    def agg(self, func, *args, **kwargs):
        result = self._gb.agg(func, *args, **kwargs)
        return ps.from_pandas(result)

    def apply(self, func, *args, **kwargs):
        result = self._gb.apply(func, *args, **kwargs)
        return ps.from_pandas(result)

    def progress_apply(self, func, *args, **kwargs):
        from tqdm.auto import tqdm

        tqdm.pandas()
        result = self._gb.progress_apply(func, *args, **kwargs)
        return ps.from_pandas(result)


def groupby(
    self: ps.DataFrame,
    by: Union[str, List[str]],
    *args,
    **kwargs,
) -> _PandasGroupBy:
    """Group ``self`` by ``by`` returning a wrapper around pandas ``GroupBy``.

    This implementation converts the DataFrame to pandas eagerly which is
    suitable for small datasets and test environments.
    """

    pdf = self.to_pandas()
    gb = pdf.groupby(by, *args, **kwargs)
    return _PandasGroupBy(gb)


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
