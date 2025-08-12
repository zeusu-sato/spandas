import os
import sys


def test_import_and_version():
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    import spandas

    assert hasattr(spandas, "__version__")

