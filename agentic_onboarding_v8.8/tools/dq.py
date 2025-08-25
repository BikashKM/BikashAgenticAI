import re, pandas as pd

def profile(df):
    return {
        "rows":len(df),
        "columns":{
            c:{
                "dtype":str(df[c].dtype),
                "nulls":int(df[c].isna().sum()),
                "unique":int(df[c].nunique())
            } for c in df.columns
        }
    }

def propose_rules(df):
    rules=[]
    for c in df.columns:
        if df[c].isna().mean()<0.05: rules.append({"type":"not_null","column":c})
        if df[c].nunique()==len(df): rules.append({"type":"unique","column":c})
        if re.search("email",c,re.I): rules.append({"type":"regex","column":c,"pattern":r"^[^@\s]+@[^@\s]+\.[^@\s]+$"})
    return rules

def apply_rules(df, rules):
    res=[]
    for r in rules:
        c=r["column"]; t=r["type"]
        if t=="not_null": bad=int(df[c].isna().sum()); res.append({"rule":r,"passed":bad==0,"detail":f"{bad} nulls"})
        elif t=="unique": bad=len(df)-df[c].nunique(); res.append({"rule":r,"passed":bad==0,"detail":f"{bad} dups"})
        elif t=="regex":  bad=int((~df[c].astype(str).str.match(r["pattern"])).sum()); res.append({"rule":r,"passed":bad==0,"detail":f"{bad} mismatches"})
    return df,res
