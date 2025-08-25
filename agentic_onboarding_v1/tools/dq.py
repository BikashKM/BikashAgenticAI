
import re, math, pandas as pd, numpy as np
from typing import Dict, Any, List, Tuple

def profile(df: pd.DataFrame) -> Dict[str, Any]:
    prof = {"rows": len(df), "columns": {}}
    for col in df.columns:
        s = df[col]
        colprof = {
            "dtype": str(s.dtype),
            "nulls": int(s.isna().sum()),
            "unique": int(s.nunique()),
        }
        if pd.api.types.is_numeric_dtype(s):
            colprof.update({
                "min": float(np.nanmin(s.values)) if len(s.dropna()) else None,
                "max": float(np.nanmax(s.values)) if len(s.dropna()) else None,
                "mean": float(np.nanmean(s.values)) if len(s.dropna()) else None,
            })
        prof["columns"][col] = colprof
    return prof

def propose_rules(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rules = []
    for col in df.columns:
        s = df[col]
        # Propose not_null if few nulls
        nulls = s.isna().mean()
        if nulls < 0.05:
            rules.append({"type": "not_null", "column": col})
        # Unique candidate for id-like columns
        if s.nunique() == len(s):
            rules.append({"type": "unique", "column": col})
        # Range for numeric
        if pd.api.types.is_numeric_dtype(s):
            if len(s.dropna()) > 0:
                rules.append({"type": "range", "column": col, "min": float(s.min()), "max": float(s.max())})
        # Simple email regex if column name suggests
        if re.search(r'email', col, re.I):
            rules.append({"type": "regex", "column": col, "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"})
    return rules

def apply_rules(df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    results = []
    for r in rules:
        t = r["type"]
        col = r.get("column")
        passed = True
        detail = ""
        if t == "not_null":
            bad = df[col].isna().sum()
            passed = bad == 0
            detail = f"{bad} nulls"
        elif t == "unique":
            bad = len(df) - df[col].nunique()
            passed = bad == 0
            detail = f"{bad} duplicates"
        elif t == "range":
            mn, mx = r.get("min"), r.get("max")
            bad = ((df[col] < mn) | (df[col] > mx)).sum()
            passed = bad == 0
            detail = f"{bad} out of range"
        elif t == "regex":
            pat = re.compile(r.get("pattern"))
            bad = (~df[col].astype(str).str.match(pat)).sum()
            passed = bad == 0
            detail = f"{bad} regex mismatches"
        elif t == "set_membership":
            allowed = set(r.get("allowed", []))
            bad = (~df[col].isin(allowed)).sum()
            passed = bad == 0
            detail = f"{bad} not in {sorted(list(allowed))}"
        elif t == "freshness":
            # Requires timestamp column; here we accept anything parsable
            ts = pd.to_datetime(df[col], errors="coerce")
            lag = r.get("max_lag_minutes", 60)
            bad = (pd.Timestamp.utcnow() - ts).dt.total_seconds() / 60.0 > lag
            bad = bad.sum()
            passed = bad == 0
            detail = f"{bad} rows stale"
        else:
            passed = False
            detail = "Unknown rule"
        results.append({"rule": r, "passed": bool(passed), "detail": detail})
    return df, results
