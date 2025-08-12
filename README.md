## Spandas for Databricks

Lightweight helpers to use pandas on Databricks without breaking the default
runtime. The package offers:

- Optional progress bars via `tqdm` using the familiar
  `df.progress_apply(...)` syntax.
- Simple conversion helpers between `pyspark.sql.DataFrame` and
  `pandas.DataFrame` via `spandas.to_pandas` and `spandas.to_spark`.

### Installation on Databricks

```python
%pip install -U -c https://raw.githubusercontent.com/zeusu-sato/spandas/main/constraints.txt \
  "spandas @ git+https://github.com/zeusu-sato/spandas.git"
%pip install -U "spandas[perf] @ git+https://github.com/zeusu-sato/spandas.git"  # optional tqdm
```

### Usage

```python
import pandas as pd
import spandas

# Optional tqdm progress bars
pd.Series(range(3)).progress_apply(lambda x: x)

# Spark ↔ pandas conversion
# sdf: pyspark.sql.DataFrame, pdf: pandas.DataFrame
# pdf = spandas.to_pandas(sdf)
# sdf = spandas.to_spark(pdf)
```
