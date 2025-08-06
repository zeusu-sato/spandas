# spandas/enhanced/plot/boxplot.py

"""
spandas.enhanced.plot.boxplot: Boxplot interface using matplotlib via pandas fallback.

This module provides a `.boxplot()` method for pandas-on-Spark DataFrames by converting
them to pandas and using matplotlib. Spark does not natively support box plots.
"""

__all__ = ["boxplot"]

from typing import Any
import pyspark.pandas as ps


def boxplot(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Draw a boxplot from the DataFrame's columns using pandas and matplotlib.

    NOTE:
        This method always converts to pandas internally because Spark has no native boxplot support.

    Args:
        *args, **kwargs: Passed to pandas' .boxplot() method.

    Returns:
        Any: matplotlib object.
    """
    return self.to_pandas().boxplot(*args, **kwargs)
