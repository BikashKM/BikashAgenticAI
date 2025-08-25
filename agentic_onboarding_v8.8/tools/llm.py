import os, re, pandas as pd, json
from typing import Optional, Dict, Any, List

def have_llm() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))

def narrative(history:list, text:str)->str:
    # Placeholder for richer chain-of-thought styling; keep deterministic
    return text

def infer_bk_from_profile(df: pd.DataFrame):
    candidates = []
    for c in df.columns:
        s = df[c]; score = 0
        if re.search(r'(?:^|_)id$', c): score += 2
        if s.isna().sum()==0: score += 1
        if s.nunique()==len(s): score += 2
        if s.nunique() > 0.7*len(s): score += 1
        candidates.append((score, c))
    candidates.sort(reverse=True)
    top = [c for sc, c in candidates if sc > 0]
    return top[:2] if top else []

def llm_route(prompt: str, history: List[tuple]) -> Optional[Dict[str, Any]]:
    """
    If OPENAI_API_KEY is set, call the LLM to extract fields.
    Returns:
      {
        dataset: str|None,
        source_uri: str|None,
        scope: 'landing'|'integration'|'dwh'|'e2e'|None,
        sttm_choice: 'suggested'|'different'|None,
        bk: [str]|None,
        scd_integration: 1|2|None,
        scd_dwh: 1|2|None,
        show_profile: bool|None,
        show_dq: bool|None
      }
    """
    if not have_llm():
        return None
    try:
        from openai import OpenAI
        client = OpenAI()
        sys_prompt = (
            "You are a routing function for a data-onboarding assistant. "
            "From the user's last message, extract a JSON object with keys: "
            "dataset (string or null), source_uri (string or null), "
            "scope ('landing'|'integration'|'dwh'|'e2e' or null), "
            "sttm_choice ('suggested'|'different' or null), "
            "bk (array of strings or null), "
            "scd_integration (1|2|null), scd_dwh (1|2|null), "
            "show_profile (bool), show_dq (bool). "
            "Answer ONLY with valid JSON."
        )
        msgs = [{"role":"system","content":sys_prompt}]
        for role, content in history[-4:]:
            msgs.append({"role": ("user" if role=="user" else "assistant"), "content": content})
        msgs.append({"role":"user","content": prompt})
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=msgs,
            response_format={"type":"json_object"}
        )
        data = resp.choices[0].message.content
        return json.loads(data)
    except Exception:
        return None
