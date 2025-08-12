import importlib.util
import sys

import spandas  # noqa: F401


def test_no_dask_swifter():
    for name in ("dask", "swifter"):
        assert name not in sys.modules
        assert importlib.util.find_spec(name) is None
