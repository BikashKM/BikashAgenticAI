import pandas as pd, hashlib

def scd_type1_merge(existing, incoming, bk):
    if existing is None or len(existing)==0: return incoming.copy()
    if not bk: raise ValueError("SCD1 merge requires business keys.")
    e=existing.set_index(bk); i=incoming.set_index(bk)
    e.update(i); return e.reset_index()

def scd_type2_merge(existing, incoming, bk, eff_from="effective_from", eff_to="effective_to", current_flag="is_current"):
    if not bk: raise ValueError("SCD2 merge requires business keys.")
    ts=pd.Timestamp.utcnow().normalize()
    if existing is None or len(existing)==0:
        df=incoming.copy(); df[eff_from]=ts; df[eff_to]=pd.NaT; df[current_flag]=True; return df
    non_keys=[c for c in incoming.columns if c not in bk]
    def h(sr): return hashlib.md5("|".join([str(sr[c]) for c in non_keys]).encode()).hexdigest()
    inc=incoming.copy(); inc["_h"]=inc.apply(h,axis=1)
    ex=existing.copy(); ex["_h"]=ex[non_keys].apply(h,axis=1) if non_keys else ""
    merged=[]; ex_cur=ex[ex[current_flag]==True] if current_flag in ex.columns else ex.assign(**{current_flag:True})
    keys=set(ex_cur[bk].apply(tuple,axis=1).tolist()) if len(ex_cur)>0 else set()
    for _,r in inc.iterrows():
        key=tuple(r[bk].tolist()) if isinstance(bk,list) else (r[bk],)
        if key in keys:
            cur=ex_cur[ex_cur[bk].apply(tuple,axis=1)==key].iloc[0]
            if cur.get("_h","")!=r["_h"]:
                ex.loc[(ex[bk].apply(tuple,axis=1)==key)&(ex.get(current_flag,True)==True), current_flag]=False
                if eff_to in ex.columns:
                    ex.loc[(ex[bk].apply(tuple,axis=1)==key)&(ex[current_flag]==False)&(ex[eff_to].isna()), eff_to]=ts
                nr=r.drop(labels=["_h"]).to_dict(); nr[eff_from]=ts; nr[eff_to]=pd.NaT; nr[current_flag]=True; merged.append(nr)
        else:
            nr=r.drop(labels=["_h"]).to_dict(); nr[eff_from]=ts; nr[eff_to]=pd.NaT; nr[current_flag]=True; merged.append(nr)
    if merged: ex=pd.concat([ex.drop(columns=[c for c in ["_h"] if c in ex.columns]), pd.DataFrame(merged)], ignore_index=True, sort=False)
    else:
        if "_h" in ex.columns: ex=ex.drop(columns=["_h"])
    return ex
