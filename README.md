## 🇯🇵 Spandas - Databricks 向けの軽量拡張

**Spandas** は、PySpark の pandas API (`from pyspark import pandas as ps`) をベースに、pandas のような使いやすさで Spark 上の DataFrame 操作を強化する、Databricks ランタイム向けの軽量ライブラリです。

### 特徴

- pandasのような `.apply()`, `.agg()`, `.groupby()` などの操作をSpark上で再現
- `.plot()`, `.hist()`, `.boxplot()` による可視化（pandas経由）
- `to_pandas=False` によるSparkネイティブなベストエフォート処理
- `.loc`, `.iloc`, `.T`, `.pivot`, `.melt` など、使い慣れたAPIをサポート

### インストール

Databricks では次のコマンドだけで利用できます:

```bash
pip install spandas
```

オプション機能:

- **Dask 連携:** `pip install "spandas[dask_legacy]"`
- **ローカル Spark 検証:** `pip install "spandas[spark]"`

> **注意:** 本ライブラリは PySpark 3.5 系および pandas 1.x 系に対応しています。

### 使用例

```python
from spandas import Spandas
from pyspark import pandas as ps

psdf = ps.read_csv("sample.csv")
sdf = Spandas(psdf)

sdf = sdf.dropna()
sdf["new_col"] = sdf["val"].apply(lambda x: x**2)
sdf.plot()
```

### 構成ディレクトリ

- `original/` ... 元のpandas-on-Spark互換関数の退避
- `enhanced/` ... 機能ごとの強化関数群（apply, selection, mathstats など）
- `spandas.py` ... Spandasクラス本体（拡張機能を統合）

---

## 🇺🇸 Spandas - Lightweight Extensions for Databricks

**Spandas** extends PySpark's pandas API (`from pyspark import pandas as ps`) to provide a pandas-like experience on Spark.

### Features

- Familiar pandas-style API on Spark: `.apply()`, `.agg()`, `.groupby()`, etc.
- Plotting via `.plot()`, `.hist()`, `.boxplot()` (backed by pandas/matplotlib)
- Best-effort native Spark execution with `to_pandas=False`
- Support for `.loc`, `.iloc`, `.T`, `.pivot`, `.melt`, and more

### Installation

On Databricks just run:

```bash
pip install spandas
```

Optional extras:

- **Dask integration:** `pip install "spandas[dask_legacy]"`
- **Local Spark testing:** `pip install "spandas[spark]"`

> **Note:** The package targets PySpark 3.5.x and pandas 1.x (Databricks Runtime compatible).

### Example

```python
from spandas import Spandas
from pyspark import pandas as ps

psdf = ps.read_csv("sample.csv")
sdf = Spandas(psdf)

sdf = sdf.dropna()
sdf["new_col"] = sdf["val"].apply(lambda x: x**2)
sdf.plot()
```

### Project Structure

- `original/` - Backups of original pandas-on-Spark methods
- `enhanced/` - Feature-specific enhancements (apply, selection, mathstats, etc.)
- `spandas.py` - Main class that binds all enhanced functionality

---

## 📄 License / ライセンス

- English: This project is licensed under the **MIT License** – see the [LICENSE](./LICENSE) file for details.
- 日本語: 本プロジェクトは **MITライセンス** のもとで公開されています。詳細は [LICENSE](./LICENSE) ファイルをご確認ください。
