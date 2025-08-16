import os, pandas as pd, sqlite3
def read_source_csv(path:str)->pd.DataFrame:
    return pd.read_csv(path)
def read_target_csv(path:str)->pd.DataFrame|None:
    return pd.read_csv(path) if os.path.exists(path) else None
def write_target_csv(df:pd.DataFrame, path:str):
    os.makedirs(os.path.dirname(path), exist_ok=True); df.to_csv(path, index=False)
def read_source_sqlite(db_path:str, table:str)->pd.DataFrame:
    con=sqlite3.connect(db_path); return pd.read_sql_query(f"SELECT * FROM {table}", con)
def read_target_sqlite(db_path:str, table:str)->pd.DataFrame|None:
    if not os.path.exists(db_path): return None
    con=sqlite3.connect(db_path)
    try: return pd.read_sql_query(f"SELECT * FROM {table}", con)
    except Exception: return None
def write_target_sqlite(df:pd.DataFrame, db_path:str, table:str, if_exists:str='replace'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con=sqlite3.connect(db_path); df.to_sql(table, con, if_exists=if_exists, index=False)
