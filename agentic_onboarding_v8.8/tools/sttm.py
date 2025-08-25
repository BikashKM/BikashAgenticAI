from typing import Dict, Any, List, Optional
import pandas as pd, os, glob, re

EXPECTED_COLS=[
    "Source Schema","Source Table","Source Column","Business Logic",
    "Transformation","Target Schema","Target Table","Target Column"
]

def _resolve_dir(default_dir:str)->str:
    env=os.environ.get("STTM_DIR")
    if env and os.path.isdir(env): return env
    here=os.path.dirname(__file__)
    cand=os.path.normpath(os.path.join(here, "..", default_dir))
    if os.path.isdir(cand): return cand
    if os.path.isdir(default_dir): return default_dir
    os.makedirs(default_dir, exist_ok=True); return default_dir

def list_sttm_files(dir_path: Optional[str]=None):
    d=_resolve_dir(dir_path or "sttm")
    files=sorted(glob.glob(os.path.join(d, "*.xlsx")))
    return files

def load_sttm_excel(path:str)->Dict[str,Any]:
    if not path: raise ValueError("STTM path is empty. Choose an STTM first.")
    df=pd.read_excel(path, sheet_name="STTM")
    for c in EXPECTED_COLS:
        if c not in df.columns: raise ValueError(f"Missing column: {c}")
    return {
        "int_map": df[df["Target Schema"].str.lower()=="int"].copy(),
        "dw_map":  df[df["Target Schema"].str.lower()=="dw"].copy(),
        "all": df
    }

def target_table_from_map(df:pd.DataFrame)->str:
    vals=df["Target Table"].dropna().unique()
    return vals[0] if len(vals)>0 else "unknown_table"

def extract_bk_from_business_logic(sttm_df: pd.DataFrame) -> List[str]:
    mask = sttm_df["Business Logic"].astype(str).str.contains("business key", case=False, na=False)
    bks=set()
    for _, r in sttm_df[mask].iterrows():
        col = str(r.get("Target Column") or r.get("Source Column"))
        if col and col.lower()!="nan":
            bks.add(col)
    return list(bks)

def discover_datasets(samples_dir="data/samples", sttm_dir="sttm"):
    data = {}
    samples_dir = _resolve_dir(samples_dir)
    sttm_dir = _resolve_dir(sttm_dir)
    for csv in sorted(glob.glob(os.path.join(samples_dir, "*.csv"))):
        base = os.path.splitext(os.path.basename(csv))[0]
        cands = [p for p in sorted(glob.glob(os.path.join(sttm_dir, "*.xlsx")))]
        data[base] = {"csv": csv, "sttm": cands}
    return data

def normpath(p:str)->str:
    return os.path.normpath(p).replace("\\","/")

def suggest_sttm_for_dataset(dataset:str, sttm_dir:str="sttm")->Optional[str]:
    if not dataset: return None
    sttm_dir=_resolve_dir(sttm_dir); ds = dataset.strip().lower()
    stems=[ds]
    if ds.endswith("ies"): stems.append(ds[:-3]+"y")
    if ds.endswith("es"):  stems.append(ds[:-2])
    if ds.endswith("s"):   stems.append(ds[:-1])
    files = sorted(glob.glob(os.path.join(sttm_dir, "*.xlsx")))
    if not files: return None
    # Score by filename and by Target Table tokens
    def score(path:str)->int:
        b=os.path.basename(path).lower().replace(".xlsx","")
        sc=0
        for stem in stems:
            if stem and stem in b: sc+=3
            if stem and stem.rstrip("s") in b: sc+=2
        try:
            df=pd.read_excel(path, sheet_name="STTM")
            tts = " ".join(df["Target Table"].astype(str).tolist()).lower()
            for stem in stems:
                if stem and stem in tts: sc+=2
        except Exception:
            pass
        return sc
    best=sorted(files, key=lambda p: (score(p), -len(os.path.basename(p))), reverse=True)
    return best[0] if best and score(best[0])>0 else None

# Demo STTM writers (only used if no STTM exists)
def write_customer_sttm_xlsx(path):
    cols = EXPECTED_COLS
    rows = []
    rows.append(["landing","landing_customers","customer_id","business key","", "int","int_customer_dim_stage","customer_id"])
    for c in ["email","first_name","last_name","country","state","zip","birthdate","signup_ts","is_active"]:
        rows.append(["landing","landing_customers",c,"pass through","", "int","int_customer_dim_stage",c])
    rows.append(["landing","landing_customers","first_name","full name","(first_name + ' ' + last_name).str.strip()", "int","int_customer_dim_stage","full_name"])
    rows.append(["landing","landing_customers","email","extract domain","email.str.split('@').str[-1]", "int","int_customer_dim_stage","email_domain"])
    for c in ["customer_id","email","first_name","last_name","country","state","zip","birthdate","signup_ts","is_active","full_name","email_domain"]:
        rows.append(["int","int_customer_dim_stage",c,"pass through","", "dw","dw_customer_dim",c])
    df = pd.DataFrame(rows, columns=cols)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="STTM")

def write_sales_sttm_xlsx(path):
    cols = EXPECTED_COLS
    rows = []
    rows.append(["landing","landing_orders","order_id","business key","", "int","int_sales_fact_stage","order_id"])
    for c in ["customer_id","order_ts","amount","currency","status"]:
        rows.append(["landing","landing_orders",c,"pass through","", "int","int_sales_fact_stage",c])
    for c in ["order_id","customer_id","order_ts","amount","currency","status"]:
        rows.append(["int","int_sales_fact_stage",c,"pass through","", "dw","dw_sales_fact",c])
    rows.append(["int","int_sales_fact_stage","order_ts","derive year","(order_ts.dt.year).astype(int)","dw","dw_sales_fact","order_year"])
    rows.append(["int","int_sales_fact_stage","order_ts","derive month","(order_ts.dt.month).astype(int)","dw","dw_sales_fact","order_month"])
    df = pd.DataFrame(rows, columns=cols)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="STTM")
