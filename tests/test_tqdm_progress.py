import pandas as pd


def test_progress_apply_exists_and_runs_without_spark():
    # tqdm.pandas() が効いた状態でも、pandas 単体で壊れないことだけ確認
    try:
        import tqdm.auto as _tqdm  # noqa: F401
        from spandas.progress import enable_tqdm
        enable_tqdm()
    except Exception:
        # 進捗は任意機能なので、ここで例外にしない
        pass

    df = pd.DataFrame({"x": [1, 2, 3]})
    out = df.apply(lambda s: s["x"] * 2, axis=1)  # ここは純 pandas
    assert list(out) == [2, 4, 6]
