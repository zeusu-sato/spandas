import os
import sys
import pandas.testing as tm
import pytest

spark_enabled = os.getenv("SPANDAS_ENABLE_SPARK_TESTS") == "1"
pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(not spark_enabled, reason="spark tests disabled by default"),
]

try:
    from pyspark import pandas as ps
    from pyspark.sql import SparkSession
except Exception:
    pytest.skip("pyspark not available", allow_module_level=True)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from spandas.enhanced.selection import iloc

SparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()


def test_iloc_with_integer_col_idx_selects_correct_column():
    df = ps.DataFrame({'A': [1, 2], 'B': [3, 4], 'C': [5, 6]})
    result = iloc(df, slice(0, 2), 1, to_pandas=True)
    expected = df.to_pandas().iloc[0:2, 1]
    tm.assert_series_equal(result.reset_index(drop=True), expected.reset_index(drop=True))
