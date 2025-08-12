# Changelog

## 0.1.2
- Make Dask optional (extras: dask_legacy) to keep pandas<2 compatibility
- Remove pyspark from core dependencies
- Align requirements/constraints for CI stability

## 0.1.1

- Drop `pyspark` from default dependencies; provide `spandas[local]` extra for local verification.
- Add `constraints.txt` for Databricks-friendly dependency pinning.
- Add runtime guard for missing `pyspark` with helpful instructions.
- Add CI workflow validating installation via constraints.
- Update packaging metadata for release 0.1.1.
