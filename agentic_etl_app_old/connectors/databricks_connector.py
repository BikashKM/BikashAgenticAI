from __future__ import annotations
from databricks import sql as dbsql
import pandas as pd
from typing import Optional, List
from utils.sql_merge import build_merge_sql

class DatabricksConnector:
    def __init__(self, host: str, http_path: str, token: str, catalog: Optional[str] = None, schema: Optional[str] = None):
        self.host = host; self.http_path = http_path; self.token = token; self.catalog = catalog; self.schema = schema

    def _conn(self):
        return dbsql.connect(server_hostname=self.host, http_path=self.http_path, access_token=self.token)

    def qualified(self, table: str) -> str:
        parts = []
        if self.catalog: parts.append(self.catalog)
        if self.schema: parts.append(self.schema)
        parts.append(table)
        return ".".join(parts)

    def test(self) -> str:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("select current_user()")
            return str(cur.fetchone())

    def execute(self, sql: str):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(sql)

    def read_sql(self, sql: str) -> pd.DataFrame:
        with self._conn() as con:
            return pd.read_sql(sql, con)

    def write_df(self, df: pd.DataFrame, table: str, overwrite: bool = False, batch_size: int = 5000) -> int:
        qtable = self.qualified(table)
        with self._conn() as con:
            cur = con.cursor()
            if overwrite:
                cur.execute(f"drop table if exists {qtable}")
            cols = ", ".join([f"`{c}` string" for c in df.columns])
            cur.execute(f"create table if not exists {qtable} ({cols}) using delta")
            placeholders = ", ".join(["?"] * len(df.columns))
            insert_sql = f"insert into {qtable} values ({placeholders})"
            rows = [tuple(None if pd.isna(v) else str(v) for v in r) for r in df.itertuples(index=False, name=None)]
            for i in range(0, len(rows), batch_size):
                cur.executemany(insert_sql, rows[i:i+batch_size])
        return len(df)

    def merge_from_staging(self, target_table: str, staging_table: str, keys: List[str], columns: List[str]):
        qtarget = self.qualified(target_table)
        qstaging = self.qualified(staging_table)
        sql = build_merge_sql("databricks", qtarget, qstaging, keys, columns)
        self.execute(sql)

    def copy_into_from_s3(self, table: str, s3_uri: str):
        qtable = self.qualified(table)
        sql = f"""COPY INTO {qtable}
FROM '{s3_uri}'
FILEFORMAT = CSV
FORMAT_OPTIONS('header'='true')"""
        self.execute(sql)
