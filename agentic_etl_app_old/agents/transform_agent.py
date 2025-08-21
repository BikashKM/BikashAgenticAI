from __future__ import annotations
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

class TransformAgent:
    def __init__(self, use_llm: bool = False):
        self.use_llm = use_llm

    def integrate(self, base_df: pd.DataFrame, ref_df_map: Dict[str, pd.DataFrame], join_instructions: Dict[str, Any]) -> pd.DataFrame:
        df = base_df.copy()
        left_on = join_instructions.get('left_on', [])
        for ref in join_instructions.get('refs', []):
            name = ref.get('name')
            how = ref.get('how','left')
            rkey = ref.get('df_key')
            if name in ref_df_map and left_on:
                df = df.merge(ref_df_map[name], how=how, left_on=left_on[0], right_on=rkey)
        return df

    def scd_load(self, existing: Optional[pd.DataFrame], incoming: pd.DataFrame, scd_type: str, keys: List[str]) -> pd.DataFrame:
        scd_type = (scd_type or 'SCD1').upper()
        if scd_type == 'SCD1' or not keys:
            if existing is None or existing.empty:
                return incoming.copy()
            merged = pd.concat([existing, incoming], ignore_index=True)
            merged = merged.drop_duplicates(subset=keys, keep='last')
            return merged
        elif scd_type == 'SCD2':
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            if existing is None or existing.empty:
                out = incoming.copy()
                out['effective_start'] = now
                out['effective_end'] = None
                out['is_current'] = True
                return out
            out = existing.copy()
            inc = incoming.set_index(keys)
            out2 = out.copy(); out2_idx = out2.set_index(keys)
            common = out2_idx.index.intersection(inc.index)
            changed_mask = (out2_idx.loc[common].fillna('') != inc.loc[common].reindex(out2_idx.loc[common].columns, axis=1).fillna('')).any(axis=1)
            to_close = out2_idx.loc[common][changed_mask].index
            if len(to_close)>0:
                out.loc[out.set_index(keys).index.isin(to_close) & (out['is_current']==True), ['effective_end','is_current']] = [now, False]
            new_versions = incoming.copy()
            new_versions['effective_start'] = now
            new_versions['effective_end'] = None
            new_versions['is_current'] = True
            final = pd.concat([out, new_versions], ignore_index=True)
            final.sort_values(keys + ['effective_start'], inplace=True)
            return final
        else:
            if existing is None or existing.empty:
                return incoming.copy()
            merged = pd.concat([existing, incoming], ignore_index=True)
            merged = merged.drop_duplicates(subset=keys, keep='last')
            return merged
