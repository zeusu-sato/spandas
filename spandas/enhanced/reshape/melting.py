# spandas/enhanced/reshape/melting.py

"""
spandas.enhanced.reshape.melting: Implements melt and wide_to_long functions for reshaping.

These functions provide long-format transformation capabilities similar to pandas,
with optional `to_pandas=True` for full fidelity.
"""

__all__ = ["melt", "wide_to_long"]

from typing import Optional, List
import pandas as pd
import pyspark.pandas as ps


def melt(
    self: ps.DataFrame,
    id_vars: Optional[List[str]] = None,
    value_vars: Optional[List[str]] = None,
    var_name: Optional[str] = None,
    value_name: str = "value",
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Unpivot a DataFrame from wide to long format.

    Args:
        id_vars (Optional[List[str]]): Columns to use as identifier variables.
        value_vars (Optional[List[str]]): Columns to unpivot.
        var_name (Optional[str]): Name to use for the 'variable' column.
        value_name (str): Name to use for the 'value' column.
        to_pandas (bool): Whether to use pandas melt with full fidelity.

    Returns:
        ps.DataFrame: Melted long-format DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas().melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )
        return ps.from_pandas(pdf)
    else:
        return self.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )


def wide_to_long(
    self: ps.DataFrame,
    stubnames: List[str],
    i: str,
    j: str,
    sep: str = '',
    suffix: str = '\\d+',
    to_pandas: bool = False
) -> ps.DataFrame:
    """
    Reshape wide-format DataFrame to long format.

    Args:
        stubnames (List[str]): List of stub names.
        i (str): Column to use as identifier variable.
        j (str): Column name for new sub-variable.
        sep (str): Separator between stubname and suffix.
        suffix (str): Regular expression pattern for suffixes.
        to_pandas (bool): Whether to use pandas for exact transformation.

    Returns:
        ps.DataFrame: Reshaped long-format DataFrame.
    """
    if to_pandas:
        pdf = self.to_pandas()
        long_pdf = pd.wide_to_long(pdf, stubnames=stubnames, i=i, j=j, sep=sep, suffix=suffix).reset_index()
        return ps.from_pandas(long_pdf)
    else:
        # Best-effort: fallback to melt-style long format
        melted = self.to_pandas().melt(id_vars=[i], var_name=j, value_name="value")
        return ps.from_pandas(melted)
