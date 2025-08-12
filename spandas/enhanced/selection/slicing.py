# spandas/enhanced/selection/slicing.py

"""
spandas.enhanced.selection.slicing: Implements row slicing utilities including head, tail, and sample
with optional pandas fallback for full fidelity.
"""

from typing import Union, Optional
from spandas.compat import ps
import pandas as pd

__all__ = ["head", "tail", "sample"]

def head(self: ps.DataFrame, n: int = 5, to_pandas: bool = False) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Return the first `n` rows.

    Args:
        self (ps.DataFrame): The input DataFrame.
        n (int): Number of rows to return.
        to_pandas (bool): Use to_pandas for exact behavior.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Resulting top rows.
    """
    if to_pandas:
        return self.to_pandas().head(n)
    return self.limit(n)


def tail(self: ps.DataFrame, n: int = 5, to_pandas: bool = False) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Return the last `n` rows.

    Args:
        self (ps.DataFrame): The input DataFrame.
        n (int): Number of rows to return.
        to_pandas (bool): Use to_pandas for exact behavior.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Resulting bottom rows.
    """
    if to_pandas:
        return self.to_pandas().tail(n)
    count = self.count()
    start = max(0, count - n)
    import pyspark.sql.functions as F

    df = self.withColumn("__row_id", F.monotonically_increasing_id())
    result = df.filter(F.col("__row_id") >= start)
    return result.drop("__row_id")


def sample(
    self: ps.DataFrame,
    n: Optional[int] = None,
    frac: Optional[float] = None,
    replace: bool = False,
    random_state: Optional[int] = None,
    to_pandas: bool = False
) -> Union[ps.DataFrame, pd.DataFrame]:
    """
    Return a random sample of items.

    Args:
        self (ps.DataFrame): Input DataFrame.
        n (int, optional): Number of items to return.
        frac (float, optional): Fraction of items to return.
        replace (bool): Sample with or without replacement.
        random_state (int, optional): Seed for random sampling.
        to_pandas (bool): Use to_pandas for full pandas behavior.

    Returns:
        Union[ps.DataFrame, pd.DataFrame]: Sampled DataFrame.
    """
    if to_pandas:
        return self.to_pandas().sample(n=n, frac=frac, replace=replace, random_state=random_state)

    seed = random_state if random_state is not None else None
    if frac is not None:
        return self.sample(frac=frac, replace=replace, random_state=seed)
    elif n is not None:
        total = self.count()
        frac = n / total if total > 0 else 0
        return self.sample(frac=frac, replace=replace, random_state=seed)
    else:
        raise ValueError("Either `n` or `frac` must be specified.")
