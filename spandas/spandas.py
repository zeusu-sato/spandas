# spandas/spandas.py

"""Core Spandas classes.

This module provides thin wrappers around :mod:`pyspark.pandas` objects so that
users who are familiar with the pandas API can interact with Spark DataFrames
and Series using *exactly* the same syntax.  Methods that exist in
``pyspark.pandas`` are delegated there directly; for any attributes that are
missing, we fall back to converting the object to pandas and calling the pandas
implementation.  The result is then converted back into a Spandas object when
possible.

Only a light layer of glue code is implemented here – the heavy lifting is
still handled by pandas-on-Spark.  This keeps the behaviour close to pandas
while avoiding the need to manually re‑implement every method.
"""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd
from spandas.compat import ps

from spandas.original import backup as original
from spandas.enhanced import (
    apply_ext,
    join_ext,
    selection,
    aggregation,
    missing,
    mathstats,
    reshape,
    plot_ext,
)


class Spandas(ps.DataFrame):
    """
    Spandas: An enhanced DataFrame class combining pandas-like ease of use
    with Spark's scalability.  Heavy computations can fall back to pandas for
    small datasets when requested.
    """

    # --------- Original Methods ---------
    apply_original         = original.apply_original
    applymap_original      = original.applymap_original
    map_original           = original.map_original
    agg_original           = original.agg_original
    transform_original     = original.transform_original
    groupby_original       = original.groupby_original
    filter_original        = original.filter_original
    select_original        = original.select_original
    dropna_original        = original.dropna_original
    fillna_original        = original.fillna_original
    join_original          = original.join_original
    merge_original         = original.merge_original
    pivot_original         = original.pivot_original
    melt_original          = original.melt_original
    loc_original           = original.loc_original
    iloc_original          = original.iloc_original
    T_original             = original.T_original

    # --------- Enhanced Apply ---------
    apply                  = apply_ext.apply
    applymap               = apply_ext.applymap
    transform              = apply_ext.transform
    progress_apply         = apply_ext.progress_apply
    pipe                   = apply_ext.pipe
    where                  = apply_ext.where
    mask                   = apply_ext.mask
    combine                = apply_ext.combine
    combine_first          = apply_ext.combine_first

    # --------- Enhanced Selection ---------
    loc                    = selection.loc
    iloc                   = selection.iloc
    at                     = selection.at
    iat                    = selection.iat
    xs                     = selection.xs
    head                   = selection.head
    tail                   = selection.tail
    sample                 = selection.sample
    isin                   = selection.isin  # from filter_mask
    where                  = apply_ext.where  # logically mask/where
    mask                   = apply_ext.mask

    # --------- Enhanced Reshaping ---------
    pivot                  = reshape.pivot
    pivot_table            = reshape.pivot_table
    stack                  = reshape.stack
    unstack                = reshape.unstack
    melt                   = reshape.melt
    wide_to_long           = reshape.wide_to_long
    explode                = reshape.explode
    get_dummies            = reshape.get_dummies
    transpose              = reshape.transpose

    # --------- Enhanced Missing ---------
    dropna                 = missing.dropna
    fillna                 = missing.fillna

    # --------- Enhanced Math/Stats ---------
    corr                   = mathstats.corr
    cov                    = mathstats.cov
    interpolate            = mathstats.interpolate
    resample               = mathstats.resample
    asfreq                 = mathstats.asfreq
    rolling                = mathstats.rolling
    expanding              = mathstats.expanding

    # --------- Enhanced Aggregation ---------
    agg                    = aggregation.agg
    groupby                = aggregation.groupby
    describe               = aggregation.describe

    # --------- Enhanced Join ---------
    join                   = join_ext.join
    merge                  = join_ext.merge

    # --------- Enhanced Plot ---------
    plot                   = plot_ext.plot
    hist                   = plot_ext.hist
    boxplot                = plot_ext.boxplot

    @property
    def T(self):
        """
        Shortcut for transpose (i.e., df.T is equivalent to df.transpose()).
        Uses pandas for accurate transpose.
        """
        from spandas.enhanced.reshape.reshaping import transpose
        return transpose(self)

    # ------------------------------------------------------------------
    # Helpers to keep pandas-like API
    # ------------------------------------------------------------------
    def __getattr__(self, item: str) -> Any:  # pragma: no cover - thin wrapper
        """Fallback attribute access.

        If ``pyspark.pandas`` implements the requested attribute we delegate to
        it.  Otherwise the DataFrame is converted to pandas and the pandas
        implementation is used.  Results that are ``DataFrame``/``Series`` are
        converted back into Spandas objects so that chaining continues to work.
        """

        if hasattr(ps.DataFrame, item):
            attr = getattr(ps.DataFrame, item)

            def wrapper(*args, **kwargs):
                result = attr(self, *args, **kwargs)
                return _as_spandas(result)

            return wrapper

        pd_df = self.to_pandas()
        pd_attr = getattr(pd_df, item)
        if callable(pd_attr):

            def pd_wrapper(*args, **kwargs):
                result = pd_attr(*args, **kwargs)
                return _as_spandas(result)

            return pd_wrapper

        return pd_attr

    def __getitem__(self, key: Any) -> Any:  # pragma: no cover - thin wrapper
        result = super().__getitem__(key)
        return _as_spandas(result)


class SpandasSeries(ps.Series):
    """Series counterpart of :class:`Spandas` with pandas-like fallbacks."""

    def __getattr__(self, item: str) -> Any:  # pragma: no cover - thin wrapper
        if hasattr(ps.Series, item):
            attr = getattr(ps.Series, item)

            def wrapper(*args, **kwargs):
                result = attr(self, *args, **kwargs)
                return _as_spandas(result)

            return wrapper

        pd_series = self.to_pandas()
        pd_attr = getattr(pd_series, item)
        if callable(pd_attr):

            def pd_wrapper(*args, **kwargs):
                result = pd_attr(*args, **kwargs)
                return _as_spandas(result)

            return pd_wrapper

        return pd_attr

    def __getitem__(self, key: Any) -> Any:  # pragma: no cover - thin wrapper
        result = super().__getitem__(key)
        return _as_spandas(result)


def _as_spandas(obj: Any) -> Any:
    """Convert pandas-on-Spark objects to Spandas wrappers."""

    if isinstance(obj, ps.DataFrame) and not isinstance(obj, Spandas):
        obj.__class__ = Spandas
    elif isinstance(obj, ps.Series) and not isinstance(obj, SpandasSeries):
        obj.__class__ = SpandasSeries
    return obj
