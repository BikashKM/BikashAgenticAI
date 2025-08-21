
from __future__ import annotations
import pandas as pd, re
from typing import Dict, Any, List
RE_JOIN = re.compile(r"JOIN\s+([^.]+)\.([^.]+)\.([^\s]+)\s*=\s*([^.]+)\.([^.]+)\.([^\s]+)", re.IGNORECASE)
RE_SCD = re.compile(r"SCD([123])", re.IGNORECASE)
RE_PK = re.compile(r"primary\s*key", re.IGNORECASE)
REQUIRED_COLS = ["Source Schema","Source Table","Source Column","Business Logic","Transformation","Target Schema","Target Table","Target Column"]
def load_sttm_excel(path_or_bytes) -> pd.DataFrame:
    if isinstance(path_or_bytes,(bytes,bytearray)):
        return pd.read_excel(pd.io.common.BytesIO(path_or_bytes))
    return pd.read_excel(path_or_bytes)
def validate_sttm(df: pd.DataFrame):
    miss=[c for c in REQUIRED_COLS if c not in df.columns]
    if miss: raise ValueError(f"STTM Excel missing columns: {miss}")
    for c in REQUIRED_COLS:
        if c in df.columns: df[c]=df[c].fillna("")
def get_scd_for_target(df: pd.DataFrame, target_schema: str, target_table: str, default_scd: str="SCD1")->str:
    rows=df[(df["Target Schema"].str.lower()==target_schema.lower()) & (df["Target Table"].str.lower()==target_table.lower())]
    for _,r in rows.iterrows():
        m=RE_SCD.search(str(r["Business Logic"]))
        if m: return f"SCD{m.group(1)}"
    return default_scd
def get_keys_for_target(df: pd.DataFrame, target_schema:str, target_table:str)->list:
    keys=[]; rows=df[(df["Target Schema"].str.lower()==target_schema.lower()) & (df["Target Table"].str.lower()==target_table.lower())]
    for _,r in rows.iterrows():
        if RE_PK.search(str(r["Business Logic"])):
            col=str(r["Target Column"]).strip() or str(r["Source Column"]).strip()
            if col and col not in keys: keys.append(col)
    return keys
def build_integration_plan(df: pd.DataFrame, target_table: str)->dict:
    rows=df[(df["Target Schema"].str.lower()=="integration") & (df["Target Table"].str.lower()==target_table.lower())]
    if rows.empty: raise ValueError(f"No STTM rows for integration target {target_table}")
    left_on, refs, projection = [], [], []
    for _,r in rows.iterrows():
        src_schema=str(r["Source Schema"]).strip().lower()
        src_table=str(r["Source Table"]).strip()
        src_col=str(r["Source Column"]).strip()
        tgt_col=str(r["Target Column"]).strip() or src_col
        biz=str(r["Business Logic"])
        projection.append((src_schema, src_table, src_col, tgt_col))
        m=RE_JOIN.search(biz)
        if m:
            l_schema,l_table,l_col,r_schema,r_table,r_col = m.groups()
            if l_schema.lower()=="landing":
                if l_col not in left_on: left_on.append(l_col)
                refs.append({"name": r_table, "df_key": r_col, "how":"left"})
    return {"left_on": left_on, "refs": refs, "projection": projection}
def project_columns(df_map: Dict[tuple, pd.DataFrame], projection):
    import pandas as pd
    base_key=None
    for (schema, table) in df_map.keys():
        if schema=="landing": base_key=(schema,table); break
    if base_key is None: base_key=next(iter(df_map.keys()))
    base_df=df_map[base_key]
    out=pd.DataFrame()
    for src_schema, src_table, src_col, tgt_col in projection:
        key=(src_schema.lower(), src_table)
        if key not in df_map: continue
        s=df_map[key]
        if src_col not in s.columns: continue
        out[tgt_col]=s[src_col]
    return out if not out.empty else base_df.copy()
def build_dwh_targets(df: pd.DataFrame)->list:
    return sorted(df[df["Target Schema"].str.lower()=="dwh"]["Target Table"].str.strip().unique().tolist())
