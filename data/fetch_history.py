from __future__ import annotations

from pathlib import Path

import pandas as pd

from data.okx_rest import OKXPublicClient


def download_history_bars(
    inst_id: str,
    bar: str,
    bars: int,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    client = OKXPublicClient()
    df = client.download_history(inst_id=inst_id, bar=bar, bars=bars, start=start, end=end)
    if df.empty:
        return df
    return df.sort_index().drop_duplicates()


def download_1h_history(
    inst_id: str,
    bars: int,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    return download_history_bars(inst_id=inst_id, bar="1H", bars=bars, start=start, end=end)


def save_candles_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = df.reset_index().rename(columns={"index": "ts"})
    output.to_csv(path, index=False)
