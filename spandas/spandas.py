# spandas/spandas.py

"""
spandas.spandas: Defines the core Spandas class that extends pandas-on-Spark
with enhanced functionality and pandas-like interface.

This class supports enhanced apply, selection, aggregation, reshaping,
missing value handling, joins, math/stats, and plotting, with minimal
use of .to_pandas() (only for plotting and optional in complex methods).
"""

import pyspark.pandas as ps
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
    with Spark's scalability, powered by pandas-on-Spark and swifter.
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
    pipe                   = apply_ext.pipe
    where                  = apply_ext.where
    mask                   = apply_ext.mask
    combine                = apply_ext.combine
    combine_first          = join_ext.combine_first  # logically related to join

    # --------- Enhanced Selection ---------
    loc                    = selection.loc
    iloc                   = selection.iloc
    at                     = selection.at
    iat                    = selection.iat
    xs                     = selection.xs
    head                   = selection.head
    tail                   = selection.tail
    sample                 = selection.sample
    isin                   = selection.isin

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
