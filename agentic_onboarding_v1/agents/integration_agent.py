
import pandas as pd, yaml
from typing import Dict, Any
from ..tools.sttm import load_sttm, generate_sttm_from_brd
from ..tools.transforms import scd_type1_merge, scd_type2_merge
from ..tools.connectors import write_sqlite, read_local_csv

def transform_to_integration(df: pd.DataFrame, sttm: Dict[str, Any]) -> pd.DataFrame:
    mappings = sttm.get("target_integration", {}).get("mappings", [])
    out = pd.DataFrame()
    for m in mappings:
        if "source" in m:
            out[m["target"]] = df[m["source"]]
        elif "expr" in m:
            out[m["target"]] = df.eval(m["expr"])
    return out

def load_integration(existing_df: pd.DataFrame, incoming_df: pd.DataFrame, sttm: Dict[str, Any]) -> pd.DataFrame:
    scd = int(sttm.get("scd_type", 1))
    bk = sttm.get("business_key", [])
    if scd == 1:
        merged = scd_type1_merge(existing_df, incoming_df, bk)
    else:
        merged = scd_type2_merge(existing_df, incoming_df, bk)
    return merged
