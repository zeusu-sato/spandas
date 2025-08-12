import importlib

import pandas as pd
import spandas  # noqa: F401 - triggers tqdm patch


def test_progress_apply():
    HAS_TQDM = importlib.util.find_spec("tqdm") is not None
    has_progress = hasattr(pd.DataFrame, "progress_apply")
    if HAS_TQDM:
        assert has_progress
    else:
        assert not has_progress
