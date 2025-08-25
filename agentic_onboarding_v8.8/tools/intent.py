import re

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def parse_dataset_from_text(text: str) -> str | None:
    t = _clean(text)
    m = re.search(r"(onboard|load|ingest)\s+([a-z0-9_ \-]+?)(?:\s+data|\s+dataset|\s+table|\s+source)?(?:\s|$)", t)
    if m:
        cand = m.group(2).strip()
        cand = re.sub(r"\b(end\s*to\s*end|e2e|landing|integration|dwh|warehouse|only|with|and|then)\b.*$", "", cand).strip()
        cand = cand.replace(" ", "_")
        if cand:
            return cand
    for noun in ["customers", "customer", "orders", "order"]:
        if re.search(rf"\b{noun}\b", t):
            return "customers" if "customer" in noun else "orders"
    return None

def parse_action(text: str) -> dict | None:
    t = _clean(text)
    out = {}
    if re.search(r"\bend\s*to\s*end\b|\be2e\b|\bfull\s*(pipeline|run)\b", t):
        out["action"] = "e2e"
    elif "warehouse" in t or "dwh" in t:
        out["action"] = "dwh"
    elif "integration" in t:
        out["action"] = "integration"
    elif "landing" in t or "landing only" in t:
        out["action"] = "landing"
    if re.search(r"\bskip\s*(dq|data\s*quality)\b", t):
        out["run_dq"] = False
    elif re.search(r"\b(run|do)\s*(dq|data\s*quality)\b|\bwith\s*dq\b", t):
        out["run_dq"] = True
    if "after dq go to dwh" in t or "after dq to dwh" in t:
        out["after_dq"] = "dwh"
    return out or None

def parse_bk(text: str):
    t = _clean(text)
    m = re.search(r"\b(bk|business\s*key)\s*(is|=)\s*([a-z0-9_,\s]+)", t)
    if m:
        cols = [c.strip() for c in m.group(3).split(",") if c.strip()]
        return cols or None
    m2 = re.search(r"\b([a-z0-9_]+)\b\s+is\s+(the\s+)?business\s*key", t)
    if m2:
        return [m2.group(1)]
    return None

def parse_scd(text: str):
    t = _clean(text)
    if re.search(r"\bscd\s*2\b|\bscd2\b|\btype\s*2\b", t):
        return 2
    if re.search(r"\bscd\s*1\b|\bscd1\b|\btype\s*1\b", t):
        return 1
    if "scd" in t:
        if "2" in t: return 2
        if "1" in t: return 1
    return None

def parse_source_uri(text: str) -> str | None:
    t = text.strip()
    m = re.search(r"(file://[^\s]+|[A-Za-z]:[\\/][^\s]+|(?:\./|\.\./|/)?[A-Za-z0-9_\-./\\]+)", t)
    if m:
        cand = m.group(1)
        if re.search(r"\.(csv|xlsx|xls|parquet)$", cand, re.I):
            return cand
    return None
