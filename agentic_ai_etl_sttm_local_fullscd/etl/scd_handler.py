import pandas as pd
from typing import List, Optional

def deduplicate_source(src_out: pd.DataFrame,
                       business_key: str,
                       strategy: str = "fail",
                       timestamp_col: str | None = None) -> pd.DataFrame:
    if business_key not in src_out.columns:
        raise KeyError(f"Business key '{business_key}' not present in transformed output.")
    dup_mask = src_out.duplicated(subset=[business_key], keep=False)
    if not dup_mask.any():
        return src_out
    if strategy == "fail":
        dups = src_out[dup_mask].copy()
        raise ValueError(f"Duplicate business keys found for '{business_key}'. Count={len(dups)}. Sample keys: {list(dups[business_key].dropna().unique()[:10])}")
    if strategy == "keep_first":
        return src_out.drop_duplicates(subset=[business_key], keep="first").reset_index(drop=True)
    if strategy == "keep_last":
        return src_out.drop_duplicates(subset=[business_key], keep="last").reset_index(drop=True)
    if strategy == "by_timestamp":
        if not timestamp_col or timestamp_col not in src_out.columns:
            raise ValueError("Timestamp column not found for 'by_timestamp' strategy.")
        ts = pd.to_datetime(src_out[timestamp_col], errors="coerce")
        ordered = src_out.assign(__ts=ts).sort_values(["__ts"])
        deduped = ordered.drop_duplicates(subset=[business_key], keep="last").drop(columns=["__ts"])
        return deduped.reset_index(drop=True)
    raise ValueError(f"Unknown deduplication strategy: {strategy}")

def _ensure_cols(df: pd.DataFrame, cols: List[str], default=None):
    for c in cols:
        if c not in df.columns:
            df[c] = default
    return df

def scd_type_1(src_out: pd.DataFrame,
               tgt_existing: Optional[pd.DataFrame],
               audit_cols: Optional[dict] = None) -> pd.DataFrame:
    df = src_out.copy()
    if audit_cols:
        for k,v in (audit_cols or {}).items():
            df[k] = v
    return df

def _infer_tracked_if_empty(tracked_cols: List[str], out_cols: List[str], business_key: str) -> List[str]:
    if tracked_cols: return tracked_cols
    tech = {business_key, 'effective_start','effective_end','is_current','version','batch_id','loaded_at'}
    return [c for c in out_cols if c not in tech]

def scd_type_2(src_out: pd.DataFrame,
               tgt_existing: Optional[pd.DataFrame],
               business_key: str,
               tracked_cols: List[str] | None,
               eff_start: str='effective_start',
               eff_end: str='effective_end',
               current_flag: str='is_current',
               version_col: str='version',
               surrogate_key_col: Optional[str]=None,
               as_of: Optional[pd.Timestamp]=None,
               soft_delete: bool=True,
               load_mode: str='Snapshot',  # 'Snapshot' or 'Incremental'
               audit_cols: Optional[dict]=None) -> pd.DataFrame:
    now = as_of or pd.Timestamp.utcnow()
    src = src_out.copy()
    tracked_cols = _infer_tracked_if_empty(tracked_cols or [], list(src.columns), business_key)

    if tgt_existing is None or tgt_existing.empty:
        base = src.copy()
        base[eff_start] = now
        base[eff_end] = pd.NaT
        base[current_flag] = True
        base[version_col] = 1
        if surrogate_key_col:
            base[surrogate_key_col] = range(1, len(base)+1)
        if audit_cols:
            for k,v in (audit_cols or {}).items(): base[k] = v
        return base

    tgt = tgt_existing.copy()
    _ensure_cols(tgt, [eff_start, eff_end, current_flag, version_col], default=pd.NaT)
    tgt[current_flag] = tgt[current_flag].fillna(False).astype(bool)
    if surrogate_key_col and surrogate_key_col not in tgt.columns:
        tgt[surrogate_key_col] = pd.NA

    result = tgt.copy()
    current_df = result[result[current_flag] == True].copy()
    current_map = { r[business_key]: (i, r) for i, r in current_df.iterrows() }

    next_sk = 1
    if surrogate_key_col and surrogate_key_col in result.columns:
        try:
            next_sk = int(pd.to_numeric(result[surrogate_key_col], errors='coerce').max()) + 1
        except Exception:
            next_sk = 1

    src_keys = set(src[business_key].tolist())

    for _, newr in src.iterrows():
        bk = newr[business_key]
        if bk not in current_map:
            ins = newr.copy()
            ins[eff_start] = now; ins[eff_end] = pd.NaT; ins[current_flag] = True; ins[version_col] = 1
            if surrogate_key_col: ins[surrogate_key_col] = next_sk; next_sk += 1
            if audit_cols:
                for k,v in (audit_cols or {}).items(): ins[k] = v
            result = pd.concat([result, pd.DataFrame([ins])], ignore_index=True)
        else:
            idx, cur = current_map[bk]
            changed = False
            for c in tracked_cols:
                if c in newr.index and c in cur.index:
                    if pd.isna(cur[c]) and pd.isna(newr[c]): continue
                    if (cur[c] != newr[c]): changed = True; break
            if changed:
                result.loc[idx, current_flag] = False
                result.loc[idx, eff_end] = now
                new_ins = newr.copy()
                prev_ver = int(result.loc[idx, version_col]) if pd.notna(result.loc[idx, version_col]) else 1
                new_ins[version_col] = prev_ver + 1
                new_ins[eff_start] = now; new_ins[eff_end] = pd.NaT; new_ins[current_flag] = True
                if surrogate_key_col: new_ins[surrogate_key_col] = next_sk; next_sk += 1
                if audit_cols:
                    for k,v in (audit_cols or {}).items(): new_ins[k] = v
                result = pd.concat([result, pd.DataFrame([new_ins])], ignore_index=True)

    if load_mode == 'Snapshot' and soft_delete:
        cur_rows = result[result[current_flag] == True]
        to_expire_idx = cur_rows[~cur_rows[business_key].isin(src_keys)].index
        if len(to_expire_idx) > 0:
            result.loc[to_expire_idx, current_flag] = False
            result.loc[to_expire_idx, eff_end] = now

    return result

def scd_type_3(src_out: pd.DataFrame,
               tgt_existing: Optional[pd.DataFrame],
               keys: List[str],
               tracked_cols: List[str],
               prev_prefix: str='prev_',
               audit_cols: Optional[dict]=None) -> pd.DataFrame:
    """SCD3: Maintain current + previous values for tracked_cols in-place."""
    if tgt_existing is None or tgt_existing.empty:
        df = src_out.copy()
        for c in tracked_cols:
            prev = f"{prev_prefix}{c}"
            if prev not in df.columns: df[prev] = pd.NA
        if audit_cols:
            for k,v in (audit_cols or {}).items(): df[k]=v
        return df
    tgt = tgt_existing.copy()
    if isinstance(keys, list):
        idx_map = {tuple(tgt.loc[i, keys]): i for i in tgt.index}
    else:
        idx_map = {tgt.loc[i, keys]: i for i in tgt.index}
    for _, r in src_out.iterrows():
        k = tuple(r[keys]) if isinstance(keys, list) else r[keys]
        if k in idx_map:
            i = idx_map[k]
            for c in tracked_cols:
                prev = f"{prev_prefix}{c}"
                cur_val = tgt.at[i, c] if c in tgt.columns else pd.NA
                new_val = r.get(c, pd.NA)
                if (pd.isna(cur_val) and pd.isna(new_val)) or (cur_val == new_val): continue
                if prev not in tgt.columns: tgt[prev] = pd.NA
                tgt.at[i, prev] = cur_val; tgt.at[i, c] = new_val
        else:
            newr = r.copy()
            for c in tracked_cols:
                prev = f"{prev_prefix}{c}"
                if prev not in newr.index: newr[prev] = pd.NA
            tgt = pd.concat([tgt, pd.DataFrame([newr])], ignore_index=True)
    if audit_cols:
        for k,v in (audit_cols or {}).items():
            tgt[k] = v
    return tgt
