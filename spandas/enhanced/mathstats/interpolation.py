# spandas/enhanced/mathstats/interpolation.py

"""
spandas.enhanced.mathstats.interpolation: Implements interpolate method for Spandas.

Provides:
- interpolate(): Fill missing values using interpolation.
"""

from typing import Literal, Optional
import pyspark.pandas as ps
import pandas as pd


def interpolate(
    self: ps.DataFrame,
    method: Literal['linear'] = 'linear',
    axis: int = 0,
    limit: Optional[int] = None,
    limit_direction: Optional[str] = None,
    limit_area: Optional[str] = None,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Fill NaNs using interpolation method.

    Args:
        method (str): Interpolation method. Only 'linear' supported without pandas.
        axis (int): Axis to interpolate along (0 or 1).
        limit (Optional[int]): Max number of consecutive NaNs to fill.
        limit_direction (Optional[str]): Direction of fill ('forward', 'backward', or both).
        limit_area (Optional[str]): Restrict interpolation to 'inside' or 'outside'.
        to_pandas (bool): If True, use full pandas interpolate for compatibility.

    Returns:
        ps.DataFrame: Interpolated DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas()
        result = pdf.interpolate(
            method=method,
            axis=axis,
            limit=limit,
            limit_direction=limit_direction,
            limit_area=limit_area,
            *args,
            **kwargs
        )
        return ps.from_pandas(result)
    else:
        if method != "linear":
            raise ValueError("Only 'linear' interpolation is supported without to_pandas=True.")
        # Best-effort linear interpolation using fillna (approximation)
        df = self
        if limit_direction in (None, 'forward'):
            df = df.fillna(method="ffill", axis=axis)
        if limit_direction in (None, 'backward'):
            df = df.fillna(method="bfill", axis=axis)
        return df
