import spandas


def test_public_api():
    assert hasattr(spandas, "to_pandas")
    assert hasattr(spandas, "to_spark")
