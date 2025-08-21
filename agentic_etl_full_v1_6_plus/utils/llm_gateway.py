
from __future__ import annotations
import os, json, requests
DEFAULT_MODEL = os.getenv("LLM_MODEL","gpt-4o-mini")
DEFAULT_BASE = os.getenv("LLM_BASE_URL","https://api.openai.com/v1")
def infer_plan_from_prompt(user_text:str, api_key:str|None=None, model:str|None=None, base_url:str|None=None)->dict:
    API_KEY = api_key or os.getenv("OPENAI_API_KEY","")
    if not API_KEY:
        return {"use_samples": True, "steps": ["Landing","DQ","Integration","DWH","Report"], "scd_hint":"SCD1","sources":[{"name":"customers","type":"sample"},{"name":"orders","type":"sample"}]}
    prompt = f"You are an ETL planner. Emit JSON with keys use_samples (bool), steps (array), scd_hint (SCD1/2/3), sources (array of {{name,type}}). User: {user_text}"
    body = {"model": model or DEFAULT_MODEL, "messages":[{"role":"user","content":prompt}], "temperature":0.1}
    headers={"Authorization": f"Bearer {API_KEY}","Content-Type":"application/json"}
    r=requests.post(f"{base_url or DEFAULT_BASE}/chat/completions", headers=headers, json=body, timeout=60)
    r.raise_for_status()
    txt = r.json()["choices"][0]["message"]["content"]
    return json.loads(txt)
