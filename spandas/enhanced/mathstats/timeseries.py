# spandas/enhanced/mathstats/timeseries.py

"""
spandas.enhanced.mathstats.timeseries: Implements time series operations like resample, asfreq, rolling, expanding.
"""

from typing import Union, Optional, Callable
import pyspark.pandas as ps
import pandas as pd


def resample(
    self: ps.DataFrame,
    rule: str,
    on: Optional[str] = None,
    how: Union[str, Callable] = 'mean',
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Resample time series data to a new frequency.

    Args:
        rule (str): Resample frequency (e.g., 'D', 'M').
        on (Optional[str]): Column to use for datetime index.
        how (str or callable): Aggregation method.
        to_pandas (bool): Use full pandas for compatibility.

    Returns:
        ps.DataFrame: Resampled DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas()
        if on:
            pdf[on] = pd.to_datetime(pdf[on])
            pdf.set_index(on, inplace=True)
        result = pdf.resample(rule).agg(how)
        return ps.from_pandas(result)
    else:
        raise NotImplementedError("resample requires to_pandas=True for accurate results.")


def asfreq(
    self: ps.DataFrame,
    freq: str,
    method: Optional[str] = None,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Convert time series to given frequency without aggregation.

    Args:
        freq (str): Frequency string (e.g., 'D', 'H').
        method (Optional[str]): Method to use for filling holes.
        to_pandas (bool): Use pandas for compatibility.

    Returns:
        ps.DataFrame: Frequency-converted DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas()
        result = pdf.asfreq(freq, method=method)
        return ps.from_pandas(result)
    else:
        raise NotImplementedError("asfreq requires to_pandas=True for accurate behavior.")


def rolling(
    self: ps.DataFrame,
    window: int,
    min_periods: Optional[int] = None,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Provide rolling window calculations.

    Args:
        window (int): Size of the moving window.
        min_periods (int): Minimum observations in window required.
        to_pandas (bool): Use pandas for compatibility.

    Returns:
        ps.DataFrame: Result of rolling window.
    """
    if to_pandas:
        pdf = self.to_pandas()
        result = pdf.rolling(window=window, min_periods=min_periods, *args, **kwargs).mean()
        return ps.from_pandas(result)
    else:
        raise NotImplementedError("rolling requires to_pandas=True for now.")


def expanding(
    self: ps.DataFrame,
    min_periods: int = 1,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Provide expanding window calculations.

    Args:
        min_periods (int): Minimum observations required to start.
        to_pandas (bool): Use pandas for compatibility.

    Returns:
        ps.DataFrame: Result of expanding window.
    """
    if to_pandas:
        pdf = self.to_pandas()
        result = pdf.expanding(min_periods=min_periods, *args, **kwargs).mean()
        return ps.from_pandas(result)
    else:
        raise NotImplementedError("expanding requires to_pandas=True for now.")
