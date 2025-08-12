# spandas/enhanced/selection/indexing.py

"""
spandas.enhanced.selection.indexing: Provides pandas-like indexing methods such as loc, iloc, at, iat, and xs.
Supports both best-effort Spark-based logic and fallback to to_pandas for full compatibility.
"""

from typing import Union, Any, Tuple, Optional
import pandas as pd
from spandas.compat import ps

__all__ = ["loc", "iloc", "at", "iat", "xs"]

def loc(
    self: ps.DataFrame,
    row_condition: Union[str, ps.Series],
    columns: Union[str, list, None] = None
) -> ps.DataFrame:
    """
    Simulate pandas-like .loc[] behavior using Spark filter and select.

    Args:
        self (ps.DataFrame): The input DataFrame.
        row_condition (Union[str, ps.Series]): A condition string or boolean Series.
        columns (Union[str, list, None]): Column(s) to select.

    Returns:
        ps.DataFrame: Filtered and selected DataFrame.
    """
    df = self
    if isinstance(row_condition, str):
        df = df.filter(row_condition)
    elif isinstance(row_condition, ps.Series):
        df = df[row_condition]
    if columns is not None:
        df = df[columns]
    return df


def iloc(
    self: ps.DataFrame,
    row_idx: Union[int, slice],
    col_idx: Union[int, slice, None] = None,
    to_pandas: bool = False
) -> Union[pd.DataFrame, ps.DataFrame]:
    """
    Simulate pandas-like .iloc[] behavior.

    Args:
        self (ps.DataFrame): The input DataFrame.
        row_idx (Union[int, slice]): Row index or slice.
        col_idx (Union[int, slice, None]): Column index or slice.
        to_pandas (bool): If True, use to_pandas() for exact behavior.

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Subset DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas()
        return pdf.iloc[row_idx, col_idx] if col_idx is not None else pdf.iloc[row_idx]

    cols = list(self.columns)
    if col_idx is not None:
        if isinstance(col_idx, int):
            selected_cols = [cols[col_idx]]
        else:
            selected_cols = cols[col_idx]
    else:
        selected_cols = cols

    import pyspark.sql.functions as F

    indexed = self.withColumn("__row_id", F.monotonically_increasing_id())
    if isinstance(row_idx, int):
        filtered = indexed.filter(F.col("__row_id") == row_idx)
    elif isinstance(row_idx, slice):
        start = row_idx.start or 0
        stop = row_idx.stop
        filtered = indexed.filter((F.col("__row_id") >= start) & (F.col("__row_id") < stop))
    else:
        filtered = indexed
    return filtered.select(*selected_cols)


def at(self: ps.DataFrame, row_idx: int, col_name: str, to_pandas: bool = False) -> Any:
    """
    Access a single value using label-based lookup (best-effort via index).

    Args:
        self (ps.DataFrame): Input DataFrame.
        row_idx (int): Row index (integer).
        col_name (str): Column label.
        to_pandas (bool): If True, use full pandas behavior.

    Returns:
        Any: The value at the specified row and column.
    """
    if to_pandas:
        return self.to_pandas().at[row_idx, col_name]
    
    # Best-effort using monotonic ID
    import pyspark.sql.functions as F

    df = self.withColumn("__row_id", F.monotonically_increasing_id())
    val_df = df.filter(F.col("__row_id") == row_idx).select(col_name).limit(1)
    result = val_df.toPandas()
    return result[col_name].iloc[0] if not result.empty else None


def iat(self: ps.DataFrame, row_idx: int, col_idx: int, to_pandas: bool = False) -> Any:
    """
    Access a single value using integer-location based indexing.

    Args:
        self (ps.DataFrame): Input DataFrame.
        row_idx (int): Row index (integer).
        col_idx (int): Column index (integer).
        to_pandas (bool): If True, use full pandas behavior.

    Returns:
        Any: The value at the specified position.
    """
    if to_pandas:
        return self.to_pandas().iat[row_idx, col_idx]
    
    col_name = list(self.columns)[col_idx]
    return at(self, row_idx, col_name, to_pandas=False)


def xs(
    self: ps.DataFrame,
    key: Any,
    axis: int = 0,
    level: Optional[Union[int, str]] = None,
    drop_level: bool = True,
    to_pandas: bool = False
) -> Union[ps.Series, ps.DataFrame, pd.Series, pd.DataFrame]:
    """
    Return cross-section from the DataFrame.

    Args:
        self (ps.DataFrame): Input DataFrame.
        key (Any): Label for index or columns to retrieve.
        axis (int): 0 for index (rows), 1 for columns.
        level (int or str, optional): Level to use (not supported in Spark).
        drop_level (bool): Ignored (pandas compatibility).
        to_pandas (bool): If True, use to_pandas for exact behavior.

    Returns:
        Union[ps.Series, ps.DataFrame]: Cross-section data.
    """
    if to_pandas:
        return self.to_pandas().xs(key, axis=axis, level=level, drop_level=drop_level)

    import pyspark.sql.functions as F

    if axis == 0:
        return self.filter(F.col(self.columns[0]) == key)
    elif axis == 1:
        return self[[key]]
    else:
        raise ValueError("axis must be 0 or 1")
