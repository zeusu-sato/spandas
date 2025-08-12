"""Enable tqdm integration with pandas if available."""

try:  # pragma: no cover - optional dependency
    from tqdm import tqdm

    tqdm.pandas()  # type: ignore[attr-defined]
    HAS_TQDM = True
except ImportError:  # pragma: no cover - tqdm not installed
    HAS_TQDM = False
