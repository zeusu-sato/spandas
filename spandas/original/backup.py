# spandas/original/backup.py

"""
spandas.original.backup: Backup of original pandas-on-Spark methods.

These methods preserve the unmodified behavior of the parent class
for fallback or comparison purposes.
"""

import pyspark.pandas as ps
import pandas as pd
from typing import Any, Callable, List, Union

def apply_original(self: ps.DataFrame, func: Callable, axis: int = 1, *args, **kwargs) -> Any:
    """
    Original apply method from pandas-on-Spark.
    """
    return ps.DataFrame.apply(self, func, axis=axis, *args, **kwargs)

def applymap_original(self: ps.DataFrame, func: Callable, *args, **kwargs) -> Any:
    """
    Original applymap method from pandas-on-Spark.
    """
    return ps.DataFrame.applymap(self, func, *args, **kwargs)

def map_original(self: ps.Series, func: Callable, *args, **kwargs) -> Any:
    """
    Original map method from pandas-on-Spark.
    """
    return ps.Series.map(self, func, *args, **kwargs)

def agg_original(self: ps.DataFrame, func: Union[str, List[str], Callable], *args, **kwargs) -> Any:
    """
    Original agg method from pandas-on-Spark.
    """
    return ps.DataFrame.agg(self, func, *args, **kwargs)

def transform_original(self: ps.DataFrame, func: Callable, *args, **kwargs) -> Any:
    """
    Original transform method from pandas-on-Spark.
    """
    return ps.DataFrame.transform(self, func, *args, **kwargs)

def groupby_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original groupby method from pandas-on-Spark.
    """
    return ps.DataFrame.groupby(self, *args, **kwargs)

def filter_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original filter method from pandas-on-Spark.
    """
    return ps.DataFrame.filter(self, *args, **kwargs)

def select_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original select method from pandas-on-Spark.
    """
    return ps.DataFrame.select(self, *args, **kwargs)

def dropna_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original dropna method from pandas-on-Spark.
    """
    return ps.DataFrame.dropna(self, *args, **kwargs)

def fillna_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original fillna method from pandas-on-Spark.
    """
    return ps.DataFrame.fillna(self, *args, **kwargs)

def join_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original join method from pandas-on-Spark.
    """
    return ps.DataFrame.join(self, *args, **kwargs)

def merge_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original merge method from pandas-on-Spark.
    """
    return ps.DataFrame.merge(self, *args, **kwargs)

def pivot_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original pivot method from pandas-on-Spark.
    """
    return ps.DataFrame.pivot(self, *args, **kwargs)

def melt_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original melt method from pandas-on-Spark.
    """
    return ps.DataFrame.melt(self, *args, **kwargs)

def loc_original(self: ps.DataFrame) -> Any:
    """
    loc accessor is not directly supported in pandas-on-Spark.

    Returns:
        pd.DataFrame: Full pandas DataFrame for compatibility.
    """
    return self.to_pandas()

def iloc_original(self: ps.DataFrame) -> Any:
    """
    iloc accessor is not directly supported in pandas-on-Spark.

    Returns:
        pd.DataFrame: Full pandas DataFrame for compatibility.
    """
    return self.to_pandas()

def T_original(self: ps.DataFrame) -> pd.DataFrame:
    """
    Transpose the DataFrame using to_pandas() as a fallback.

    Returns:
        pd.DataFrame: Transposed DataFrame.
    """
    return self.to_pandas().T

def sort_values_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original sort_values method from pandas-on-Spark.

    Args:
        *args: Positional arguments passed to sort_values.
        **kwargs: Keyword arguments passed to sort_values.

    Returns:
        Any: Sorted DataFrame.
    """
    return ps.DataFrame.sort_values(self, *args, **kwargs)


def sort_index_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original sort_index method from pandas-on-Spark.

    Returns:
        Any: DataFrame sorted by index.
    """
    return ps.DataFrame.sort_index(self, *args, **kwargs)


def rename_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original rename method from pandas-on-Spark.

    Returns:
        Any: DataFrame with renamed labels.
    """
    return ps.DataFrame.rename(self, *args, **kwargs)


def set_index_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original set_index method from pandas-on-Spark.

    Returns:
        Any: DataFrame with a new index.
    """
    return ps.DataFrame.set_index(self, *args, **kwargs)


def reset_index_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original reset_index method from pandas-on-Spark.

    Returns:
        Any: DataFrame with the index reset.
    """
    return ps.DataFrame.reset_index(self, *args, **kwargs)


def astype_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original astype method from pandas-on-Spark.

    Returns:
        Any: DataFrame with updated types.
    """
    return ps.DataFrame.astype(self, *args, **kwargs)


def drop_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original drop method from pandas-on-Spark.

    Returns:
        Any: DataFrame with rows or columns dropped.
    """
    return ps.DataFrame.drop(self, *args, **kwargs)


def duplicated_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original duplicated method from pandas-on-Spark.

    Returns:
        Any: Series of booleans indicating duplicates.
    """
    return ps.DataFrame.duplicated(self, *args, **kwargs)


def nunique_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original nunique method from pandas-on-Spark.

    Returns:
        Any: Number of unique values per column.
    """
    return ps.DataFrame.nunique(self, *args, **kwargs)


def value_counts_original(self: ps.Series, *args, **kwargs) -> Any:
    """
    Original value_counts method from pandas-on-Spark Series.

    Returns:
        Any: Series with counts of unique values.
    """
    return ps.Series.value_counts(self, *args, **kwargs)


def head_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original head method from pandas-on-Spark.

    Returns:
        Any: Top N rows of the DataFrame.
    """
    return ps.DataFrame.head(self, *args, **kwargs)


def tail_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original tail method from pandas-on-Spark.

    Returns:
        Any: Bottom N rows of the DataFrame.
    """
    return ps.DataFrame.tail(self, *args, **kwargs)


def info_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original info method from pandas-on-Spark.

    Returns:
        None
    """
    return ps.DataFrame.info(self, *args, **kwargs)


def describe_original(self: ps.DataFrame, *args, **kwargs) -> Any:
    """
    Original describe method from pandas-on-Spark.

    Returns:
        Any: Summary statistics of the DataFrame.
    """
    return ps.DataFrame.describe(self, *args, **kwargs)
