
import pandas as pd, re
from typing import Dict, Any, Tuple
from ..tools import dq as dqtools
from ..tools.connectors import read_local_csv, read_s3_csv, write_sqlite
from ..tools.transforms import cast_types

def detect_source(uri: str) -> str:
    if uri.startswith("s3://"): return "s3"
    if uri.startswith("file://"): return "file"
    if uri.endswith(".csv"): return "file"
    return "unknown"

def read_source(uri: str) -> pd.DataFrame:
    typ = detect_source(uri)
    if typ == "file":
        path = uri.replace("file://", "")
        return read_local_csv(path)
    elif typ == "s3":
        return read_s3_csv(uri)
    else:
        raise RuntimeError(f"Unsupported source: {uri}")

def propose_type_fixes(df: pd.DataFrame) -> Dict[str, str]:
    proposals = {}
    for col in df.columns:
        s = df[col]
        if re.search(r"date$", col, re.I):
            proposals[col] = "date"
        elif re.search(r"ts|time|timestamp", col, re.I):
            proposals[col] = "datetime"
        elif s.dtype == "object" and s.str.match(r"^\d+$", na=False).mean() > 0.9:
            proposals[col] = "int"
    return proposals

def land(uri: str, integration_db: str, landing_table: str, log) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = read_source(uri)
    prof = dqtools.profile(df)
    proposals = propose_type_fixes(df)
    return df, {"profile": prof, "proposals": proposals, "landing_table": landing_table}
