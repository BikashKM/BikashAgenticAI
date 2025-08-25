
import pandas as pd, numpy as np, hashlib, time
from typing import Dict, Any, List
from .connectors import write_sqlite, read_sqlite

def cast_types(df: pd.DataFrame, proposals: Dict[str, str]) -> pd.DataFrame:
    for col, typ in proposals.items():
        if typ == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        elif typ == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif typ == "int":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif typ == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif typ == "str":
            df[col] = df[col].astype(str)
    return df

def scd_type1_merge(existing: pd.DataFrame, incoming: pd.DataFrame, bk: List[str]) -> pd.DataFrame:
    # Overwrite by business key
    if existing is None or len(existing)==0:
        return incoming.copy()
    merged = existing.set_index(bk).combine_first(incoming.set_index(bk)).reset_index()
    # Prefer incoming non-null values
    incoming_set = incoming.set_index(bk)
    for col in incoming.columns:
        if col not in bk:
            merged[col] = incoming_set[col].combine_first(merged[col])
    return merged

def scd_type2_merge(existing: pd.DataFrame, incoming: pd.DataFrame, bk: List[str], eff_from="effective_from", eff_to="effective_to", current_flag="is_current") -> pd.DataFrame:
    ts = pd.Timestamp.utcnow().normalize()
    if existing is None or len(existing)==0:
        df = incoming.copy()
        df[eff_from] = ts
        df[eff_to] = pd.NaT
        df[current_flag] = True
        return df
    # Determine changes by hash of non-key cols
    non_keys = [c for c in incoming.columns if c not in bk]
    def row_hash(row): 
        return hashlib.md5(("|".join([str(row[c]) for c in non_keys])).encode()).hexdigest()
    inc = incoming.copy()
    inc["_hash"] = inc.apply(row_hash, axis=1)
    ex = existing.copy()
    ex["_hash"] = ex[non_keys].apply(lambda r: hashlib.md5(("|".join([str(x) for x in r.values])).encode()).hexdigest(), axis=1)
    # Align on BK
    merged = []
    ex_map = ex.groupby(bk).tail(1)  # current versions
    for _, r in inc.iterrows():
        key = tuple(r[bk].values.tolist())
        if key in set(tuple(x) for x in ex_map[bk].values.tolist()):
            cur = ex_map[ex_map[bk].apply(tuple, axis=1)==key].iloc[0]
            if cur["_hash"] != r["_hash"]:
                # Close current
                ex.loc[(ex[bk].apply(tuple, axis=1)==key) & (ex["is_current"]==True), "is_current"] = False
                ex.loc[(ex[bk].apply(tuple, axis=1)==key) & (ex["effective_to"].isna()), "effective_to"] = ts
                # Insert new
                newr = r.drop(labels=["_hash"]).to_dict()
                newr["effective_from"] = ts
                newr["effective_to"] = pd.NaT
                newr["is_current"] = True
                merged.append(newr)
        else:
            newr = r.drop(labels=["_hash"]).to_dict()
            newr["effective_from"] = ts
            newr["effective_to"] = pd.NaT
            newr["is_current"] = True
            merged.append(newr)
    if merged:
        ex = pd.concat([ex.drop(columns=["_hash"]), pd.DataFrame(merged)], ignore_index=True, sort=False)
    else:
        ex = ex.drop(columns=["_hash"])
    return ex

def apply_filters(df: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
    for f in filters or []:
        col, include = f.get("column"), f.get("include")
        if col and include:
            df = df[df[col].isin(include)]
    return df
