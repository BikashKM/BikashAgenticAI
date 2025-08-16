import os, sqlite3, pandas as pd
def read_source_csv(path:str)->pd.DataFrame: return pd.read_csv(path)
def read_source_sqlite(db_path:str, table:str)->pd.DataFrame:
    con=sqlite3.connect(db_path)
    try: return pd.read_sql_query(f"SELECT * FROM {table}", con)
    finally: con.close()
def read_target_csv(path:str):
    if not os.path.exists(path): return None
    try: return pd.read_csv(path)
    except Exception: return None
def write_target_csv(df:pd.DataFrame, path:str):
    os.makedirs(os.path.dirname(path), exist_ok=True); df.to_csv(path, index=False)
def read_target_sqlite(db_path:str, table:str):
    con=sqlite3.connect(db_path)
    try: return pd.read_sql_query(f"SELECT * FROM {table}", con)
    except Exception: return None
    finally: con.close()
def write_target_sqlite(df:pd.DataFrame, db_path:str, table:str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con=sqlite3.connect(db_path)
    try: df.to_sql(table, con, if_exists='replace', index=False)
    finally: con.close()
