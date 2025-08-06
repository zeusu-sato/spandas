# spandas/enhanced/join.py

"""
spandas.enhanced.join: Implements merge, join, concat, combine_first operations.

Provides enhanced dataframe joining and combination functionality,
with optional pandas compatibility via to_pandas.
"""

__all__ = ["merge", "join", "concat", "combine_first"]

from typing import Union, List, Optional
import pandas as pd
import pyspark.pandas as ps


def merge(
    self: ps.DataFrame,
    right: ps.DataFrame,
    how: str = "inner",
    on: Optional[Union[str, List[str]]] = None,
    left_on: Optional[Union[str, List[str]]] = None,
    right_on: Optional[Union[str, List[str]]] = None,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Merge two DataFrames using database-style joins.

    Args:
        right (ps.DataFrame): The right DataFrame to merge with.
        how (str): Type of merge to be performed.
        on, left_on, right_on: Column(s) to join on.
        to_pandas (bool): Whether to use pandas for precise compatibility.

    Returns:
        ps.DataFrame: Merged DataFrame.
    """
    if to_pandas:
        pdf_left = self.to_pandas()
        pdf_right = right.to_pandas()
        result = pd.merge(
            pdf_left, pdf_right, how=how, on=on,
            left_on=left_on, right_on=right_on, *args, **kwargs
        )
        return ps.from_pandas(result)
    else:
        return self.merge(
            right,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            *args,
            **kwargs
        )


def join(
    self: ps.DataFrame,
    other: ps.DataFrame,
    on: Optional[Union[str, List[str]]] = None,
    how: str = "left",
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Join columns of another DataFrame.

    Args:
        other (ps.DataFrame): Other DataFrame to join.
        on (Optional[Union[str, List[str]]]): Column(s) to join on.
        how (str): How to join.
        to_pandas (bool): Whether to use pandas implementation.

    Returns:
        ps.DataFrame: Joined DataFrame.
    """
    if to_pandas:
        result = self.to_pandas().join(
            other.to_pandas(), on=on, how=how, *args, **kwargs
        )
        return ps.from_pandas(result)
    else:
        return self.join(other, on=on, how=how, *args, **kwargs)


def concat(
    objs: List[ps.DataFrame],
    axis: int = 0,
    ignore_index: bool = False,
    to_pandas: bool = False,
    *args,
    **kwargs
) -> ps.DataFrame:
    """
    Concatenate a sequence of DataFrames.

    Args:
        objs (List[ps.DataFrame]): List of DataFrames to concatenate.
        axis (int): Axis to concatenate along.
        ignore_index (bool): If True, do not preserve index.
        to_pandas (bool): Use pandas for compatibility.

    Returns:
        ps.DataFrame: Concatenated DataFrame.
    """
    if to_pandas:
        pdfs = [obj.to_pandas() for obj in objs]
        result = pd.concat(pdfs, axis=axis, ignore_index=ignore_index, *args, **kwargs)
        return ps.from_pandas(result)
    else:
        return ps.concat(objs, axis=axis, ignore_index=ignore_index, *args, **kwargs)


def combine_first(
    self: ps.DataFrame,
    other: ps.DataFrame,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Update null elements with value in the same location from `other`.

    Args:
        other (ps.DataFrame): Other DataFrame to use as fill values.
        to_pandas (bool): Whether to use pandas for compatibility.

    Returns:
        ps.DataFrame: Combined DataFrame.
    """
    if to_pandas:
        result = self.to_pandas().combine_first(other.to_pandas())
        return ps.from_pandas(result)
    else:
        return self.combine_first(other)
