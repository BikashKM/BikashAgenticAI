from __future__ import annotations
import redshift_connector
import pandas as pd
from typing import Optional, List
from utils.sql_merge import build_merge_sql

class RedshiftConnector:
    def __init__(self, host: str, port: int, database: str, user: str, password: str, ssl: bool = True):
        self.params = dict(host=host, port=port, database=database, user=user, password=password, ssl=ssl)

    def _conn(self):
        return redshift_connector.connect(**self.params)

    def test(self) -> str:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("select version();")
            return str(cur.fetchone())

    def execute(self, sql: str):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(sql)
            con.commit()

    def read_sql(self, sql: str) -> pd.DataFrame:
        with self._conn() as con:
            return pd.read_sql(sql, con)

    def write_df(self, df: pd.DataFrame, table: str, overwrite: bool = False, batch_size: int = 1000):
        with self._conn() as con:
            cur = con.cursor()
            cols = ", ".join([f'"{c}" varchar' for c in df.columns])
            if overwrite:
                cur.execute(f"""drop table if exists {table}; create table {table} ({cols});""" )
            else:
                cur.execute(f"""create table if not exists {table} ({cols});""" )
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"insert into {table} values ({placeholders})"
            rows = [tuple(None if pd.isna(v) else str(v) for v in r) for r in df.itertuples(index=False, name=None)]
            for i in range(0, len(rows), batch_size):
                cur.executemany(insert_sql, rows[i:i+batch_size])
            con.commit()
            return len(rows)

    def copy_from_s3(self, table: str, s3_uri: str, iam_role: Optional[str] = None, aws_key_id: Optional[str] = None, aws_secret_key: Optional[str] = None, region: Optional[str] = None):
        with self._conn() as con:
            cur = con.cursor()
            creds = ""
            if iam_role:
                creds = f"IAM_ROLE '{iam_role}'"
            elif aws_key_id and aws_secret_key:
                creds = f"credentials 'aws_access_key_id={aws_key_id};aws_secret_access_key={aws_secret_key}'"
            region_clause = f" region '{region}'" if region else ""
            sql = f"copy {table} from '{s3_uri}' {creds}{region_clause} csv IGNOREHEADER 1 timeformat 'auto'"
            cur.execute(sql)
            con.commit()

    def merge_from_staging(self, target_table: str, staging_table: str, keys: List[str], columns: List[str]):
        sql = build_merge_sql("redshift", target_table, staging_table, keys, columns)
        self.execute(sql)
