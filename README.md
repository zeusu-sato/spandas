## 🇯🇵 Spandas - Spark上でpandasのように使える拡張DataFrame

**Spandas** は、pandas-on-Spark（pyspark.pandas）をベースに、pandasのような使いやすさと、swifterによる並列処理、matplotlib対応の可視化などを統合し、
Spark上でのDataFrame操作を強化するライブラリです。

### 特徴

- pandasのような `.apply()`, `.agg()`, `.groupby()` などの操作をSpark上で再現
- `swifter` による自動並列化
- `.plot()`, `.hist()`, `.boxplot()` による可視化（pandas経由）
- `to_pandas=False` によるSparkネイティブなベストエフォート処理
- `.loc`, `.iloc`, `.T`, `.pivot`, `.melt` など、使い慣れたAPIをサポート

### インストール

```bash
pip install git+https://github.com/zeusu-sato/spandas.git
```

### 使用例

```python
from spandas import Spandas
import pyspark.pandas as ps

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

**Spandas** extends pandas-on-Spark (pyspark.pandas) to provide a more pandas-like experience,
including easy-to-use methods, parallelism with swifter, and plotting support via matplotlib.

### Features

- Familiar pandas-style API on Spark: `.apply()`, `.agg()`, `.groupby()`, etc.
- Automatic parallelization using `swifter`
- Plotting via `.plot()`, `.hist()`, `.boxplot()` (backed by pandas/matplotlib)
- Best-effort native Spark execution with `to_pandas=False`
- Support for `.loc`, `.iloc`, `.T`, `.pivot`, `.melt`, and more

### Installation

```bash
pip install git+https://github.com/zeusu-sato/spandas.git
```

### Example

```python
from spandas import Spandas
import pyspark.pandas as ps

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
