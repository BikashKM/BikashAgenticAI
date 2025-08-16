import os, json, re
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
