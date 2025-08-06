# spandas/enhanced/plot/plot.py

"""
spandas.enhanced.plot: Plotting interface using matplotlib via pandas fallback.

This module provides a `.plot()` method for pandas-on-Spark DataFrames
by converting them to pandas and using matplotlib. Spark native plotting is unsupported.
"""

__all__ = ["plot"]

from typing import Any
import pyspark.pandas as ps


def plot(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Plot the DataFrame using pandas and matplotlib.

    NOTE:
        This method always converts to pandas internally because Spark has no native plotting.

    Args:
        *args, **kwargs: Passed to pandas plot function.

    Returns:
        Any: matplotlib.axes.Axes or array of them.
    """
    return self.to_pandas().plot(*args, **kwargs)
