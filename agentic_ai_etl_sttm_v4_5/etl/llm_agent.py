import os, json, re, pandas as pd
def _strip_code_fences(t:str)->str: return re.sub(r'^```[a-zA-Z]*\n|```$','',t.strip())
def _openai_call(messages, model, temperature, max_tokens):
    api_key=os.getenv('OPENAI_API_KEY')
    if not api_key: raise RuntimeError('Missing OPENAI_API_KEY')
    try:
        import openai; openai.api_key=api_key
        resp=openai.ChatCompletion.create(model=model,messages=messages,temperature=temperature,max_tokens=max_tokens)
        return resp.choices[0].message['content']
    except Exception:
        from openai import OpenAI
        client=OpenAI(api_key=api_key)
        resp=client.chat.completions.create(model=model,messages=messages,temperature=temperature,max_tokens=max_tokens)
        return resp.choices[0].message.content
def extract_rules_from_sttm(rows, model, temperature, max_tokens):
    sys=(
        "You are a data engineering assistant. Return ONLY valid minified JSON, no prose. "
        "Schema: {\"mappings\":[{\"source_column\":str,\"target_column\":str,\"transformation\":str,\"notes\":str}],"
        "\"validations\":[{\"column\":str,\"type\":str,\"rule\":str}],\"tracked_cols\":[str]}"
    )
    user={"role":"user","content":"STTM rows as JSON array; keys: Source Schema, Source Table, Source Column, Business Logic, Transformation, Target Schema, Target Table, Target Column. Output strictly JSON.\n\n"+json.dumps(rows,ensure_ascii=False)}
    txt=_openai_call([{"role":"system","content":sys}, user], model, temperature, max_tokens)
    txt=_strip_code_fences(txt); import json as _j; data=_j.loads(txt)
    data.setdefault('mappings',[]); data.setdefault('validations',[]); data.setdefault('tracked_cols',[])
    return data
def generate_validations_from_sttm(rows, model, temperature, max_tokens):
    sys=("Return ONLY minified JSON with 'validations':[{'column':str,'type':str,'rule':str,'severity':str,'message':str}]")
    user={"role":"user","content":"Derive data quality rules from STTM 'Business Logic' and 'Transformation'.\n\n"+json.dumps(rows,ensure_ascii=False)}
    txt=_openai_call([{"role":"system","content":sys}, user], model, temperature, max_tokens)
    import json as _j; return _j.loads(_strip_code_fences(txt))
def _df_schema(df: pd.DataFrame, sample_rows: int = 0):
    cols=[{"name":c,"dtype":str(df[c].dtype)} for c in df.columns]
    out={"columns":cols,"rows":[]}
    if sample_rows and sample_rows>0:
        out["rows"]=df.head(sample_rows).fillna("<NA>").astype(str).to_dict(orient="records")
    return out
def validate_dataframe_summary(df: pd.DataFrame, rules_json: dict, phase: str, model: str, temperature: float, max_tokens: int, sample_rows: int=50):
    schema=_df_schema(df, sample_rows=sample_rows)
    sys=(
        "You are a data quality assistant. Return ONLY minified JSON with keys:"
        " {'phase':str,'warnings':[str],'errors':[str],'checks':[{'name':str,'status':str,'details':str}]}"
    )
    user_content=json.dumps({"phase":phase,"rules":rules_json,"schema":schema}, ensure_ascii=False)
    txt=_openai_call([{"role":"system","content":sys},{"role":"user","content":user_content}], model, temperature, max_tokens)
    import json as _j; return _j.loads(_strip_code_fences(txt))
