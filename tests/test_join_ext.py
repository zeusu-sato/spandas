import os
import sys
import pytest

run_spark = os.getenv("SPANDAS_RUN_SPARK_TESTS") == "1"
pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(not run_spark, reason="spark tests disabled in CI by default"),
]

if run_spark:
    import pandas.testing as tm
    from pyspark import pandas as ps
    from pyspark.sql import SparkSession

    SparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from spandas.enhanced import join_ext


def test_merge_returns_expected_frame():
    left = ps.DataFrame({'key': [1, 2], 'value1': [10, 20]})
    right = ps.DataFrame({'key': [1, 2], 'value2': [100, 200]})
    result = join_ext.merge(left, right, on='key')
    expected = ps.DataFrame({'key': [1, 2], 'value1': [10, 20], 'value2': [100, 200]})
    tm.assert_frame_equal(
        result.sort_values('key').to_pandas().reset_index(drop=True),
        expected.sort_values('key').to_pandas().reset_index(drop=True),
    )


def test_join_returns_expected_frame():
    left = ps.DataFrame({'value1': [10, 20]}, index=[1, 2])
    right = ps.DataFrame({'value2': [100, 200]}, index=[1, 2])
    result = join_ext.join(left, right, how='inner')
    expected = ps.DataFrame({'value1': [10, 20], 'value2': [100, 200]}, index=[1, 2])
    tm.assert_frame_equal(
        result.sort_index().to_pandas(),
        expected.sort_index().to_pandas(),
    )
