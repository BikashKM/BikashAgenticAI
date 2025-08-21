from __future__ import annotations
import os
import pandas as pd

BASE_DIR = "data"

def _ensure_dirs(layer: str):
    os.makedirs(os.path.join(BASE_DIR, layer), exist_ok=True)

def write_df(df: pd.DataFrame, layer: str, name: str) -> str:
    _ensure_dirs(layer)
    path = os.path.join(BASE_DIR, layer, f"{name}.csv")
    df.to_csv(path, index=False)
    return path

def read_local_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)
