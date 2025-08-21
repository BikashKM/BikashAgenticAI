
from __future__ import annotations
import json, os, base64
STATE_DIR = os.path.join('.run_state')
STATE_PATH = os.path.join(STATE_DIR, 'session.json')
def _encode(o):
    if isinstance(o,(bytes,bytearray)):
        return {"__b64__":True,"data":base64.b64encode(o).decode('utf-8')}
    if isinstance(o,dict): return {k:_encode(v) for k,v in o.items()}
    if isinstance(o,list): return [_encode(x) for x in o]
    return o
def _decode(o):
    if isinstance(o,dict) and o.get("__b64__"):
        return base64.b64decode(o["data"].encode('utf-8'))
    if isinstance(o,dict): return {k:_decode(v) for k,v in o.items()}
    if isinstance(o,list): return [_decode(x) for x in o]
    return o
def load_state()->dict:
    if not os.path.exists(STATE_DIR): os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH,'r',encoding='utf-8') as f: raw=json.load(f)
            return _decode(raw)
        except Exception: return {}
    return {}
def save_state(state:dict)->None:
    if not os.path.exists(STATE_DIR): os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_PATH,'w',encoding='utf-8') as f: json.dump(_encode(state), f, indent=2)
