"""spandas package.

Provides minimal helpers to bridge pandas and Spark on Databricks.
"""

# Optional tqdm integration for pandas progress bars
try:  # pragma: no cover - optional dependency
    from . import _tqdm_patch  # noqa: F401
except Exception:  # pragma: no cover - silently ignore
    pass

from importlib.metadata import PackageNotFoundError, version

try:  # pragma: no cover - version retrieval
    __version__ = version("spandas")
except PackageNotFoundError:  # pragma: no cover - fallback when not installed
    __version__ = "0.0.0"

from .interop import to_pandas, to_spark

__all__ = ["to_pandas", "to_spark", "__version__"]
