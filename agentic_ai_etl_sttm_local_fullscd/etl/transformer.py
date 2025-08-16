import pandas as pd, re
def _split_args(s:str):
    parts=[]; buf=''; depth=0; inq=None
    for ch in s:
        if inq: buf+=ch; inq=None if ch==inq else inq
        else:
            if ch in ('"',"'"): inq=ch; buf+=ch
            elif ch in '([{': depth+=1; buf+=ch
            elif ch in ')]}': depth-=1; buf+=ch
            elif ch==',' and depth==0: parts.append(buf.strip()); buf=''
            else: buf+=ch
    if buf.strip(): parts.append(buf.strip())
    return parts
def _eval_expr(expr:str, df:pd.DataFrame)->pd.Series:
    e=str(expr).strip()
    if (e.startswith('"') and e.endswith('"')) or (e.startswith("'") and e.endswith("'")): return pd.Series([e[1:-1]]*len(df), index=df.index)
    m=re.match(r'^(trim|lower|upper|title)\((.+)\)$', e, re.I)
    if m:
        fn=m.group(1).lower(); inner=m.group(2).strip(); s=_eval_expr(inner, df).astype(str)
        return s.str.strip() if fn=='trim' else (s.str.lower() if fn=='lower' else (s.str.upper() if fn=='upper' else s.str.title()))
    m=re.match(r"^split\(([^,]+),\s*['\"]([^'\"]+)['\"]\)\[(\d+)\]$", e, re.I)
    if m:
        col=m.group(1).strip(); sep=m.group(2); idx=int(m.group(3))
        if col not in df.columns: raise KeyError(col)
        s = df[col].astype(str).str.split(sep)
        try: return s.str[idx]
        except Exception: return pd.Series([None]*len(df), index=df.index)
    m=re.match(r'^concat\((.*)\)$', e, re.I|re.S)
    if m:
        inner=m.group(1); parts=_split_args(inner); out=pd.Series(['']*len(df), index=df.index)
        for p in parts: out=out+_eval_expr(p, df).astype(str)
        return out
    m=re.match(r'^(mul|add)\(([^,]+),\s*([-+]?[0-9]*\.?[0-9]+)\)$', e, re.I)
    if m:
        fn=m.group(1).lower(); col=m.group(2).strip(); num=float(m.group(3))
        if col not in df.columns: raise KeyError(col)
        s=pd.to_numeric(df[col], errors='coerce'); return s*num if fn=='mul' else s+num
    m=re.match(r'^filter_year\(([^,]+),\s*(\d{4})\)$', e, re.I)
    if m:
        col=m.group(1).strip(); year=int(m.group(2))
        if col not in df.columns: raise KeyError(col)
        dt=pd.to_datetime(df[col], errors='coerce'); return df[col].where(dt.dt.year==year)
    if e in df.columns: return df[e]
    return pd.Series([e]*len(df), index=df.index)
def apply_rules(df_src:pd.DataFrame, rules_df:pd.DataFrame)->pd.DataFrame:
    out=pd.DataFrame(index=df_src.index)
    for _,r in rules_df.iterrows():
        src_cols=[c.strip() for c in str(r['Source Column']).split(',')]
        tgt=str(r['Target Column']).strip(); transform=str(r.get('Transformation','')).strip()
        if not tgt: continue
        if transform and transform.lower()!='none':
            try: out[tgt]=_eval_expr(transform, df_src)
            except Exception as ex: out[tgt]=f'ERR:{ex}'
        else:
            col=src_cols[0]; out[tgt]=df_src[col] if col in df_src.columns else pd.NA
    return out
