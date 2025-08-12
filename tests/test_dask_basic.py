import pytest
import pandas as pd

pytestmark = pytest.mark.dask

try:
    import dask.dataframe as dd
except Exception:
    pytest.skip("dask not available", allow_module_level=True)


def test_dask_dataframe_sum():
    df = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
    assert df.a.sum().compute() == 6
