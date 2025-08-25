
import os, time, uuid, yaml, re, json, contextlib, datetime as dt
from typing import Dict, Any

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def save_yaml(data: Dict[str, Any], path: str) -> None:
    with open(path, "w") as f:
        yaml.safe_dump(data, f)

def run_id() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]

@contextlib.contextmanager
def step_logger(log, label: str):
    t0 = time.time()
    log(f"⏳ {label}…")
    try:
        yield
        dt_s = time.time() - t0
        log(f"✅ {label} done in {dt_s:.2f}s")
    except Exception as e:
        dt_s = time.time() - t0
        log(f"❌ {label} failed in {dt_s:.2f}s: {e}")
        raise

def redact(s: str) -> str:
    return re.sub(r'(?i)(password|secret|token)\s*=\s*[^&\s]+', r'\1=****', s)

def html_escape(s: str) -> str:
    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
