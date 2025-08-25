import pandas as pd
from typing import Dict, Any, List
from tools.sttm import load_sttm_excel, target_table_from_map
from tools.transforms import scd_type1_merge, scd_type2_merge

def _apply_mapping(df: pd.DataFrame, sttm_map: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    for _, r in sttm_map.iterrows():
        tgt_col = r["Target Column"]; src_col = r.get("Source Column"); expr = r.get("Transformation")
        if not tgt_col or pd.isna(tgt_col): continue
        if isinstance(expr, str) and expr.strip():
            local_df = df.copy()
            try:
                out[tgt_col] = eval(expr, {}, {"df": local_df, **{c: local_df[c] for c in local_df.columns}})
            except Exception:
                out[tgt_col] = pd.eval(expr, engine="python")
        elif isinstance(src_col, str) and src_col in df.columns:
            out[tgt_col] = df[src_col]
    return out

def transform_to_integration(df: pd.DataFrame, sttm_excel_path: str) -> Dict[str, Any]:
    sttm = load_sttm_excel(sttm_excel_path); int_map = sttm["int_map"]
    out = _apply_mapping(df, int_map)
    return {"data": out, "target_table": target_table_from_map(int_map), "all": sttm["all"]}

def load_integration(existing_df: pd.DataFrame, incoming_df: pd.DataFrame, scd_type: int, business_keys: List[str]):
    bk=list(business_keys or [])
    if int(scd_type or 1)==1: return scd_type1_merge(existing_df, incoming_df, bk)
    # ensure SCD2 tech columns exist if needed
    for col in ["effective_from","effective_to","is_current"]:
        if col not in incoming_df.columns:
            incoming_df[col] = pd.NaT if "to" in col else True
    return scd_type2_merge(existing_df, incoming_df, bk)
