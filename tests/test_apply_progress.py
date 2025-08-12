import pandas as pd
import pandas.testing as tm
import pytest
from tqdm.auto import tqdm

ps = pytest.importorskip("pyspark.pandas")
SparkSession = pytest.importorskip("pyspark.sql").SparkSession
SparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()

from spandas import Spandas


def test_apply_and_groupby_progress_apply():
    df = Spandas(ps.DataFrame({"a": [1, 1, 2], "b": [1, 2, 3]}))

    # apply
    result = df.apply(lambda col: col + 1)
    expected = df.to_pandas().apply(lambda col: col + 1)
    tm.assert_frame_equal(result.to_pandas(), expected)

    # groupby.apply
    grp_res = df.groupby("a").apply(lambda g: g)
    grp_exp = df.to_pandas().groupby("a").apply(lambda g: g)
    tm.assert_frame_equal(grp_res.to_pandas(), grp_exp)

    # progress_apply
    tqdm.pandas()
    prog_res = df.progress_apply(lambda row: row, axis=1)
    prog_exp = df.to_pandas().progress_apply(lambda row: row, axis=1)
    tm.assert_frame_equal(prog_res.to_pandas(), prog_exp)

    # groupby.progress_apply
    grp_prog_res = df.groupby("a").progress_apply(lambda g: g)
    grp_prog_exp = df.to_pandas().groupby("a").progress_apply(lambda g: g)
    tm.assert_frame_equal(grp_prog_res.to_pandas(), grp_prog_exp)
