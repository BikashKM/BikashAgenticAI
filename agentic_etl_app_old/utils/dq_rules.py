from __future__ import annotations
from typing import List, Dict, Any, Optional
import pandas as pd
import re

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

def infer_primary_keys(df: pd.DataFrame) -> List[str]:
    cands = [c for c in df.columns if c.lower()=='id' or c.lower().endswith('_id')]
    return cands or []

def propose_rules(df: pd.DataFrame, primary_keys: Optional[List[str]]=None) -> List[Dict[str, Any]]:
    rules = []
    pks = primary_keys or infer_primary_keys(df)
    for pk in pks:
        if pk in df.columns:
            rules.append({'rule': f'NOT_NULL:{pk}', 'severity': 'CRITICAL'})
            rules.append({'rule': f'UNIQUE:{pk}', 'severity': 'CRITICAL'})
    for c in df.columns:
        if 'email' in c.lower():
            rules.append({'rule': f'REGEX:{c}:EMAIL', 'severity': 'MAJOR'})
        if df[c].dtype.kind in 'iuf':
            rules.append({'rule': f'TYPE_NUMERIC:{c}', 'severity': 'MAJOR'})
        rules.append({'rule': f'NULL_PCT:{c}:<=0.2', 'severity': 'MINOR'})
    return rules

def run_checks(df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    results = []
    for r in rules:
        name = r['rule']
        sev = r.get('severity', 'MINOR')
        passed = True
        msg = ''
        try:
            if name.startswith('NOT_NULL:'):
                col = name.split(':',1)[1]
                passed = not df[col].isna().any()
                msg = f'{col} has no nulls'
            elif name.startswith('UNIQUE:'):
                col = name.split(':',1)[1]
                passed = df[col].is_unique
                msg = f'{col} values are unique'
            elif name.startswith('REGEX:'):
                _, col, kind = name.split(':')
                if kind == 'EMAIL':
                    passed = df[col].fillna('').map(lambda x: bool(EMAIL_RE.match(str(x)))).all()
                    msg = f'{col} matches EMAIL format'
            elif name.startswith('TYPE_NUMERIC:'):
                col = name.split(':',1)[1]
                pd.to_numeric(df[col], errors='raise')
                msg = f'{col} numeric check passed'
            elif name.startswith('NULL_PCT:'):
                _, col, thresh = name.split(':')
                op, val = thresh[0:2], float(thresh.split('=')[-1]) if '=' in thresh else float(thresh[2:])
                pct = float(df[col].isna().mean())
                passed = pct <= val
                msg = f'{col} null pct {pct:.2f} <= {val}'
        except Exception as e:
            passed = False
            msg = f'Rule error ({name}): {e}'
        results.append({'rule': name, 'severity': sev, 'passed': bool(passed), 'message': msg})
    summary = {'total': len(results), 'passed': sum(1 for x in results if x['passed']), 'failed': sum(1 for x in results if not x['passed'])}
    return {'results': results, 'summary': summary}
