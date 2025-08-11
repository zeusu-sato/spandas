# spandas/enhanced/reshape/pivoting.py

"""
spandas.enhanced.reshape.pivoting: Pivot-related reshaping methods for Spandas.
Implements pivot, pivot_table, stack, and unstack with Spark-compatible logic
or best-effort fallbacks.
"""

from typing import Any, Optional, Union, List
from spandas.compat import ps
import pandas as pd

__all__ = ["pivot", "pivot_table", "stack", "unstack"]


def pivot(
    self: ps.DataFrame,
    index: Optional[Union[str, List[str]]] = None,
    columns: Optional[str] = None,
    values: Optional[Union[str, List[str]]] = None,
    to_pandas: bool = False
) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Pivot the DataFrame.

    Args:
        index (Optional[Union[str, List[str]]]): Column(s) to use as index.
        columns (Optional[str]): Column to use to make new columns.
        values (Optional[Union[str, List[str]]]): Values to populate new frame.
        to_pandas (bool): Use pandas pivot for exact compatibility.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Pivoted DataFrame.
    """
    if to_pandas:
        return self.to_pandas().pivot(index=index, columns=columns, values=values)
    else:
        try:
            return self.pivot(index=index, columns=columns, values=values)
        except Exception:
            return ps.from_pandas(self.to_pandas().pivot(index=index, columns=columns, values=values))


def pivot_table(
    self: ps.DataFrame,
    values: Optional[Union[str, List[str]]] = None,
    index: Optional[Union[str, List[str]]] = None,
    columns: Optional[Union[str, List[str]]] = None,
    aggfunc: Union[str, List[str]] = "mean",
    to_pandas: bool = False
) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Create a spreadsheet-style pivot table.

    Args:
        values (str or list of str): Column(s) to aggregate.
        index (str or list of str): Rows labels.
        columns (str or list of str): Column labels.
        aggfunc (str or list of str): Aggregation function(s).
        to_pandas (bool): Whether to use pandas for exact behavior.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Pivot table.
    """
    if to_pandas:
        return self.to_pandas().pivot_table(
            values=values, index=index, columns=columns, aggfunc=aggfunc
        )
    else:
        try:
            return self.pivot_table(
                values=values, index=index, columns=columns, aggfunc=aggfunc
            )
        except Exception:
            return ps.from_pandas(
                self.to_pandas().pivot_table(
                    values=values, index=index, columns=columns, aggfunc=aggfunc
                )
            )


def stack(self: ps.DataFrame, to_pandas: bool = True) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Stack the prescribed level(s) from columns to index.

    Args:
        to_pandas (bool): Use pandas to ensure full behavior.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Stacked DataFrame.
    """
    if to_pandas:
        return self.to_pandas().stack()
    else:
        # Best-effort Spark implementation not feasible
        return ps.from_pandas(self.to_pandas().stack())


def unstack(
    self: ps.DataFrame, level: Union[str, int, List[Union[str, int]]] = -1, to_pandas: bool = True
) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Unstack (pivot) level(s) from index to columns.

    Args:
        level (str, int, or list): Level(s) of index to unstack.
        to_pandas (bool): Use pandas for accurate transformation.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Unstacked DataFrame.
    """
    if to_pandas:
        return self.to_pandas().unstack(level)
    else:
        return ps.from_pandas(self.to_pandas().unstack(level))


