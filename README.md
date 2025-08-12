## 🇯🇵 Spandas - Spark上でpandasのように使える拡張DataFrame

**Spandas** は、PySpark の pandas API (`from pyspark import pandas as ps`) をベースに、pandasのような使いやすさと、matplotlib対応の可視化などを統合し、
Spark上でのDataFrame操作を強化するライブラリです。

### 特徴

- pandasのような `.apply()`, `.agg()`, `.groupby()` などの操作をSpark上で再現
- `.plot()`, `.hist()`, `.boxplot()` による可視化（pandas経由）
- `to_pandas=False` によるSparkネイティブなベストエフォート処理
- `.loc`, `.iloc`, `.T`, `.pivot`, `.melt` など、使い慣れたAPIをサポート

### インストール

Databricks での推奨手順:

```python
%pip install -U -c https://raw.githubusercontent.com/zeusu-sato/spandas/main/constraints.txt \
  "spandas @ git+https://github.com/zeusu-sato/spandas.git"
dbutils.library.restartPython()
```

トラブル時の最小インストール:

```python
%pip install -U --no-deps "spandas @ git+https://github.com/zeusu-sato/spandas.git"
dbutils.library.restartPython()
```

> **注意:** 本ライブラリは PySpark 3.5 系および pandas 1.5 系に対応しています。

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

## 🇺🇸 Spandas - Enhanced DataFrame API on Spark with Pandas-like Syntax

**Spandas** extends PySpark's pandas API (`from pyspark import pandas as ps`) to provide a more pandas-like experience,
including easy-to-use methods and plotting support via matplotlib.

### Features

- Familiar pandas-style API on Spark: `.apply()`, `.agg()`, `.groupby()`, etc.
- Plotting via `.plot()`, `.hist()`, `.boxplot()` (backed by pandas/matplotlib)
- Best-effort native Spark execution with `to_pandas=False`
- Support for `.loc`, `.iloc`, `.T`, `.pivot`, `.melt`, and more

### Installation

Recommended installation on Databricks:

```python
%pip install -U -c https://raw.githubusercontent.com/zeusu-sato/spandas/main/constraints.txt \
  "spandas @ git+https://github.com/zeusu-sato/spandas.git"
dbutils.library.restartPython()
```

Minimal install (rely on DBR-bundled deps):

```python
%pip install -U --no-deps "spandas @ git+https://github.com/zeusu-sato/spandas.git"
dbutils.library.restartPython()
```

> **Note:** The package targets PySpark 3.5.x and pandas 1.5.x (Databricks Runtime compatible).

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

### テストの実行 / Running Tests

プロジェクトのルートディレクトリで以下を実行することで、ユニットテストを実行できます。

```bash
pip install -r requirements-dev.txt
pytest
```

---

## 📄 License / ライセンス

- English: This project is licensed under the **MIT License** – see the [LICENSE](./LICENSE) file for details.
- 日本語: 本プロジェクトは **MITライセンス** のもとで公開されています。詳細は [LICENSE](./LICENSE) ファイルをご確認ください。
