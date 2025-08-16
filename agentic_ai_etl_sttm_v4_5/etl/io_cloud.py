import pandas as pd
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def databricks_engine(conf: Dict[str, Any]) -> Engine:
    host = conf.get('server_hostname')
    http_path = conf.get('http_path')
    token = conf.get('access_token')
    if not (host and http_path and token):
        raise ValueError("Databricks config requires server_hostname, http_path, access_token")
    url = f"databricks+connector://token:{token}@{host}:443/default?http_path={http_path}"
    return create_engine(url)

def snowflake_engine(conf: Dict[str, Any]) -> Engine:
    user=conf.get('user'); password=conf.get('password'); account=conf.get('account')
    warehouse=conf.get('warehouse'); database=conf.get('database'); schema=conf.get('schema'); role=conf.get('role')
    if not (user and password and account and warehouse and database and schema):
        raise ValueError("Snowflake requires user,password,account,warehouse,database,schema")
    url=(f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}" + (f"&role={role}" if role else ""))
    return create_engine(url)

def redshift_engine(conf: Dict[str, Any]) -> Engine:
    host=conf.get('host'); port=conf.get('port',5439); database=conf.get('database')
    user=conf.get('user'); password=conf.get('password')
    if not (host and database and user and password):
        raise ValueError("Redshift requires host,database,user,password")
    url=f"redshift+redshift_connector://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)

def _engine(kind:str, conf: Dict[str, Any]) -> Engine:
    if kind=='Databricks': return databricks_engine(conf)
    if kind=='Snowflake': return snowflake_engine(conf)
    if kind=='Redshift': return redshift_engine(conf)
    raise ValueError(f"Unsupported engine kind: {kind}")

def test_connection(kind:str, conf: Dict[str, Any]) -> str:
    eng=_engine(kind, conf)
    with eng.connect() as conn:
        res=conn.execute(text("SELECT 1")).scalar()
        return f"OK (SELECT 1 -> {res})"

def read_sql_table(kind:str, conf: Dict[str, Any], table:str, query: Optional[str]=None)->pd.DataFrame:
    eng=_engine(kind, conf)
    with eng.connect() as conn:
        if query and query.strip():
            return pd.read_sql_query(text(query), conn)
        return pd.read_sql_table(table, conn)

def write_sql_table(kind:str, conf: Dict[str, Any], table:str, df:pd.DataFrame, if_exists:str='replace', chunksize:int=10000):
    eng=_engine(kind, conf)
    with eng.begin() as conn:
        df.to_sql(table, conn, if_exists=if_exists, index=False, chunksize=chunksize)
