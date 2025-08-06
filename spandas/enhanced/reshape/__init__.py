# spandas/enhanced/reshape/__init__.py

"""
Initialization for reshaping-related enhanced methods.
"""

__all__ = [
    "pivoting",
    "melting",
    "reshaping",
    "pivot",
    "pivot_table",
    "stack",
    "unstack",
    "melt",
    "wide_to_long",
    "explode",
    "get_dummies",
    "transpose",
    "T"
]

from .pivoting import pivot, pivot_table, stack, unstack
from .melting import melt, wide_to_long
from .reshaping import explode, get_dummies, transpose, T  # ←ここを追加
