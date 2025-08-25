import os, re
import pandas as pd
import sqlite3

def normpath(p: str) -> str:
    return os.path.normpath(p).replace("\\", "/")

# --- Robust reader that accepts local paths or file:// URIs ---
def read_uri(uri: str) -> pd.DataFrame:
    if uri is None:
        raise ValueError("No source URI provided.")
    # file:// scheme
    if uri.startswith("file://"):
        path = uri[len("file://"):]
    # no scheme → local path (Windows or POSIX)
    elif "://" not in uri:
        path = uri
    else:
        scheme = uri.split("://", 1)[0]
        raise NotImplementedError(f"Source URI scheme not implemented yet: {scheme}")

    path = os.path.expanduser(path)
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path)
    if ext == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file extension for source: {ext}")

def write_sqlite(df: pd.DataFrame, sqlite_path: str, table: str, if_exists="replace"):
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    with sqlite3.connect(sqlite_path) as conn:
        df.to_sql(table, conn, if_exists=if_exists, index=False)

def write_layer_csv(df: pd.DataFrame, layer_dir: str, table: str) -> str:
    os.makedirs(layer_dir, exist_ok=True)
    out = os.path.join(layer_dir, f"{table}.csv")
    df.to_csv(out, index=False)
    return out
