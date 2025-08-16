import pandas as pd
REQUIRED_COLUMNS=['Source Schema','Source Table','Source Column','Business Logic','Transformation','Target Schema','Target Table','Target Column']
def read_sttm_excel(path:str)->pd.DataFrame:
    df=pd.read_excel(path, sheet_name=0)
    missing=[c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing: raise ValueError(f'STTM is missing required columns: {missing}')
    df=df[REQUIRED_COLUMNS].copy()
    for c in REQUIRED_COLUMNS: df[c]=df[c].astype(str).str.strip()
    return df
