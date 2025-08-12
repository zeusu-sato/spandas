"""Compatibility helpers for optional PySpark pandas API.

This module provides a lazily imported ``ps`` proxy that resolves to
``pyspark.pandas`` only when actually accessed.  This keeps ``spandas``
importable in environments where ``pyspark`` is not installed (e.g. CI or
lightweight environments) while still deferring to the real module when
available.  The proxy exposes attributes of ``pyspark.pandas`` transparently.
"""

from types import ModuleType
from typing import Any

import importlib


class _LazyPS(ModuleType):
    """Module-like proxy that lazily imports ``pyspark.pandas``.

    The first attribute access triggers an import of ``pyspark.pandas`` (for
    PySpark < 4) or ``pyspark.pandas`` via ``pyspark`` (for PySpark >= 4).  This
    avoids importing ``pyspark`` at module import time.
    """

    _module: ModuleType | None = None

    def _load(self) -> ModuleType:
        if self._module is None:
            try:  # PySpark < 4.x
                self._module = importlib.import_module("pyspark.pandas")
            except Exception:
                self._module = importlib.import_module("pyspark").pandas  # type: ignore[attr-defined]
        return self._module

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - trivial
        module = self._load()
        return getattr(module, name)


# Public proxy exported as ``ps``
ps = _LazyPS("ps")

__all__ = ["ps"]

