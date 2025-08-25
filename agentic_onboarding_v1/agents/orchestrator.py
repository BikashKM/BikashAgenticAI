
import re

INTENT_MAP = {
    "landing": ["land", "ingest", "load to landing", "profile", "infer", "sample"],
    "dq": ["dq", "quality", "rule", "validate", "expectation"],
    "integration": ["integrate", "transform", "sttm", "scd", "stage"],
    "dwh": ["warehouse", "dwh", "dimension", "fact", "star", "filter"],
    "report": ["report", "email", "summary", "status"]
}

def route_intent(text: str) -> str:
    t = text.lower()
    for intent, keywords in INTENT_MAP.items():
        for k in keywords:
            if k in t:
                return intent
    # defaults
    if "end-to-end" in t or "onboard" in t:
        return "landing"  # start of pipeline
    return "landing"
