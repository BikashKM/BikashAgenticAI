from __future__ import annotations
from typing import Optional, List
import uuid
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from utils.sql_merge import build_merge_sql

class SnowflakeConnector:
    def __init__(self, user: str, password: str, account: str, warehouse: str, database: str, schema: str, role: Optional[str] = None):
        self.params = dict(user=user, password=password, account=account, warehouse=warehouse, database=database, schema=schema, role=role)

    def _conn(self):
        return snowflake.connector.connect(**{k:v for k,v in self.params.items() if v})

    def test(self) -> str:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            return str(cur.fetchone())

    def execute(self, sql: str):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(sql)
            con.commit()

    def read_sql(self, sql: str) -> pd.DataFrame:
        with self._conn() as con:
            return pd.read_sql(sql, con)

    def write_df(self, df: pd.DataFrame, table: str, overwrite: bool = False) -> int:
        with self._conn() as con:
            if overwrite:
                cols = ", ".join([f'"{c}" VARCHAR' for c in df.columns])
                con.cursor().execute(f'CREATE OR REPLACE TABLE {table} ({cols})')
            success, nchunks, nrows, _ = write_pandas(con, df, table_name=table, quote_identifiers=True)
            return int(nrows)

    def merge_df(self, df: pd.DataFrame, target_table: str, keys: List[str]) -> int:
        staging = f"{target_table}__stg_{uuid.uuid4().hex[:6]}"
        with self._conn() as con:
            cur = con.cursor()
            cols = ", ".join([f'"{c}" VARCHAR' for c in df.columns])
            cur.execute(f'CREATE TABLE IF NOT EXISTS {target_table} ({cols})')
            cur.execute(f'CREATE OR REPLACE TABLE {staging} ({cols})')
            write_pandas(con, df, table_name=staging, quote_identifiers=True)
            merge_sql = build_merge_sql("snowflake", target_table, staging, keys, list(df.columns))
            cur.execute(merge_sql)
            count = pd.read_sql(f"SELECT COUNT(*) AS c FROM {target_table}", con)["c"].iloc[0]
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            con.commit()
            return int(count)

    def copy_from_s3(self, target_table: str, s3_uri: str, aws_key_id: Optional[str] = None, aws_secret_key: Optional[str] = None, file_format: str = 'TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY="\"" SKIP_HEADER=1'):
        # Default FILE_FORMAT uses double quote as the optional enclosure.
        with self._conn() as con:
            cur = con.cursor()
            creds = ""
            if aws_key_id and aws_secret_key:
                creds = f" CREDENTIALS=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')"
            sql = f"COPY INTO {target_table} FROM '{s3_uri}'{creds} FILE_FORMAT=({file_format})"
            cur.execute(sql)
            con.commit()
