
import pandas as pd
from typing import Dict, Any
from ..tools.transforms import apply_filters, scd_type1_merge, scd_type2_merge

def to_dwh(integration_df: pd.DataFrame, sttm: Dict[str, Any]) -> pd.DataFrame:
    df = integration_df.copy()
    df = apply_filters(df, sttm.get("filters"))
    return df

def load_dwh(existing_df: pd.DataFrame, incoming_df: pd.DataFrame, sttm: Dict[str, Any]) -> pd.DataFrame:
    scd = int(sttm.get("scd_type", 1))
    bk = sttm.get("business_key", [])
    if scd == 1:
        merged = scd_type1_merge(existing_df, incoming_df, bk)
    else:
        merged = scd_type2_merge(existing_df, incoming_df, bk)
    return merged
