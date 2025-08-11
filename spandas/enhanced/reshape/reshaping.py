# spandas/enhanced/reshape/reshaping.py

"""
spandas.enhanced.reshape.reshaping: Implements reshaping functions like explode and get_dummies.

These methods offer restructuring capabilities similar to pandas,
with optional pandas interoperability via to_pandas.
"""

__all__ = ["explode", "get_dummies", "transpose", "T"]

from typing import Union, List, Optional
import pandas as pd
from spandas.compat import ps


def explode(
    self: ps.DataFrame,
    column: Union[str, List[str]],
    ignore_index: bool = False,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Explode a list-like column (or columns) into multiple rows.

    Args:
        column (Union[str, List[str]]): Column or columns to explode.
        ignore_index (bool): If True, do not preserve original index.
        to_pandas (bool): Use pandas for precise behavior.

    Returns:
        ps.DataFrame: Exploded DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas().explode(column, ignore_index=ignore_index)
        return ps.from_pandas(pdf)
    else:
        return self.explode(column)


def get_dummies(
    self: ps.DataFrame,
    columns: Optional[Union[str, List[str]]] = None,
    prefix: Optional[Union[str, List[str]]] = None,
    prefix_sep: str = "_",
    dummy_na: bool = False,
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Convert categorical variables into dummy/indicator variables.

    Args:
        columns (Optional[Union[str, List[str]]]): Column names to encode.
        prefix (Optional[Union[str, List[str]]]): Prefix for new columns.
        prefix_sep (str): Separator for prefix and category.
        dummy_na (bool): Add indicator for NaNs.
        to_pandas (bool): Use pandas for full compatibility.

    Returns:
        ps.DataFrame: One-hot encoded DataFrame.
    """
    if to_pandas:
        pdf = pd.get_dummies(
            self.to_pandas(),
            columns=columns,
            prefix=prefix,
            prefix_sep=prefix_sep,
            dummy_na=dummy_na,
        )
        return ps.from_pandas(pdf)
    else:
        return ps.get_dummies(
            self,
            columns=columns,
            prefix=prefix,
            prefix_sep=prefix_sep,
            dummy_na=dummy_na,
        )


def transpose(
    self: ps.DataFrame,
    to_pandas: bool = True
) -> Union[pd.DataFrame, ps.DataFrame]:
    """
    Transpose the DataFrame.

    Args:
        to_pandas (bool): Whether to use pandas for transposing.

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Transposed DataFrame.
    """
    if to_pandas:
        return self.to_pandas().T
    else:
        # Best-effort transpose: inefficient for large frames
        columns = self.columns
        rows = self.to_spark().collect()
        transposed = {}
        for i, col in enumerate(columns):
            transposed[col] = [row[i] for row in rows]
        pdf = pd.DataFrame(transposed).transpose()
        return ps.from_pandas(pdf)


@property
def T(self: ps.DataFrame) -> Union[pd.DataFrame, ps.DataFrame]:
    """
    Shortcut for transpose (i.e., self.T is equivalent to self.transpose()).

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Transposed DataFrame.
    """
    return transpose(self)