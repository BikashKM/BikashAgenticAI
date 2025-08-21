
from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd, re
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
def infer_primary_keys(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if c.lower()=='id' or c.lower().endswith('_id')]
def propose_rules(df: pd.DataFrame, primary_keys: Optional[List[str]]=None) -> List[Dict[str, Any]]:
    rules=[]; pks=primary_keys or infer_primary_keys(df)
    for pk in pks:
        if pk in df.columns:
            rules += [{'rule': f'NOT_NULL:{pk}', 'severity':'CRITICAL'},
                      {'rule': f'UNIQUE:{pk}', 'severity':'CRITICAL'}]
    for c in df.columns:
        if 'email' in c.lower(): rules.append({'rule': f'REGEX:{c}:EMAIL', 'severity':'MAJOR'})
        if df[c].dtype.kind in 'iuf': rules.append({'rule': f'TYPE_NUMERIC:{c}', 'severity':'MAJOR'})
        rules.append({'rule': f'NULL_PCT:{c}:<=0.2', 'severity':'MINOR'})
    return rules
def _failed_rows(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    try:
        if rule.startswith('NOT_NULL:'):
            col=rule.split(':',1)[1]; return df[df[col].isna()]
        if rule.startswith('UNIQUE:'):
            col=rule.split(':',1)[1]; return df[df[col].duplicated(keep=False)]
        if rule.startswith('REGEX:'):
            _, col, kind = rule.split(':'); 
            if kind=='EMAIL': return df[~df[col].fillna('').str.match(EMAIL_RE)]
        if rule.startswith('TYPE_NUMERIC:'):
            col=rule.split(':',1)[1]; 
            pd.to_numeric(df[col], errors='raise'); return df.iloc[0:0]
        if rule.startswith('NULL_PCT:'):
            _, col, thresh = rule.split(':'); 
            val = float(thresh.split('=')[-1]) if '=' in thresh else float(thresh[2:])
            pct = float(df[col].isna().mean())
            return df.iloc[0:0] if pct <= val else df[df[col].isna()]
    except Exception:
        return df
    return df.iloc[0:0]
def run_checks(df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    results=[]
    for r in rules:
        name=r['rule']; sev=r.get('severity','MINOR')
        fail_df=_failed_rows(df, name)
        passed=fail_df.empty
        results.append({'rule':name,'severity':sev,'passed':bool(passed),'failed_count':0 if passed else len(fail_df)})
    summary={'total':len(results),'passed':sum(1 for x in results if x['passed']),'failed':sum(1 for x in results if not x['passed'])}
    return {'results':results,'summary':summary}
