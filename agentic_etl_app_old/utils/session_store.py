from __future__ import annotations
import json, os, base64
from typing import Any

STATE_DIR = os.path.join('.run_state')
STATE_PATH = os.path.join(STATE_DIR, 'session.json')

def _encode(obj: Any):
    if isinstance(obj, bytes):
        return {"__bytes__": True, "b64": base64.b64encode(obj).decode("ascii")}
    if isinstance(obj, dict):
        return {k: _encode(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_encode(x) for x in obj]
    return obj

def _decode(obj: Any):
    if isinstance(obj, dict) and obj.get("__bytes__") is True and "b64" in obj:
        try:
            return base64.b64decode(obj["b64"].encode("ascii"))
        except Exception:
            return b""
    if isinstance(obj, dict):
        return {k: _decode(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode(x) for x in obj]
    return obj

def load_state() -> dict:
    if not os.path.exists(STATE_DIR):
        os.makedirs(STATE_DIR, exist_ok=True)
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            return _decode(raw)
        except Exception:
            return {}
    return {}

def save_state(state: dict) -> None:
    if not os.path.exists(STATE_DIR):
        os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(_encode(state), f, indent=2)
