# spandas/enhanced/plot/hist.py

"""
spandas.enhanced.plot.hist: Histogram plotting interface using matplotlib via pandas fallback.

This module provides a `.hist()` method for pandas-on-Spark DataFrames by converting
them to pandas and using matplotlib. Spark does not natively support plotting histograms.
"""

__all__ = ["hist"]

from typing import Any
from spandas.compat import ps


def hist(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Plot histogram(s) of the DataFrame's numeric columns using pandas and matplotlib.

    NOTE:
        This method always converts to pandas internally because Spark has no native plotting.

    Args:
        *args, **kwargs: Passed to pandas' .hist() method.

    Returns:
        Any: matplotlib Figure or Axes object(s).
    """
    return self.to_pandas().hist(*args, **kwargs)
