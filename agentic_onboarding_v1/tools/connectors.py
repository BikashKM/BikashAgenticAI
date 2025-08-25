
import os, io, json
import pandas as pd
from typing import Optional
from .utils import ensure_dirs

# Local CSV
def read_local_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def write_local_csv(df: pd.DataFrame, path: str) -> None:
    ensure_dirs(path)
    df.to_csv(path, index=False)

# SQLite via SQLAlchemy
from sqlalchemy import create_engine, text
def sqlite_engine(path: str):
    ensure_dirs(path)
    return create_engine(f"sqlite:///{path}", future=True)

def write_sqlite(df: pd.DataFrame, db_path: str, table: str, if_exists="replace"):
    eng = sqlite_engine(db_path)
    with eng.begin() as conn:
        df.to_sql(table, conn, if_exists=if_exists, index=False)

def read_sqlite(db_path: str, sql: str) -> pd.DataFrame:
    eng = sqlite_engine(db_path)
    with eng.begin() as conn:
        return pd.read_sql_query(text(sql), conn)

# S3 (optional)
def read_s3_csv(uri: str) -> pd.DataFrame:
    try:
        import boto3
    except Exception as e:
        raise RuntimeError("boto3 not installed. Install and set AWS credentials.") from e
    if not uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")
    s3 = boto3.client("s3")
    parts = uri[5:].split("/", 1)
    bucket = parts[0]; key = parts[1]
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

# Snowflake/Redshift/Databricks placeholders (lazy)
def not_configured(name: str):
    raise RuntimeError(f"{name} connector requested but not configured. Please supply credentials and install required driver.")

def write_snowflake(*args, **kwargs): not_configured("Snowflake")
def write_redshift(*args, **kwargs): not_configured("Redshift")
def write_databricks(*args, **kwargs): not_configured("Databricks")
