
from typing import Dict, Any, List
import yaml, pandas as pd

def load_sttm(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def generate_sttm_from_brd(brd_text: str, table_name: str, columns: List[str]) -> Dict[str, Any]:
    # Very simple heuristic; in real deployment use LLM to parse BRD.
    mappings = [{"source": c, "target": c} for c in columns]
    sttm = {
        "name": table_name,
        "scd_type": 2,
        "business_key": ["id"] if "id" in [c.lower() for c in columns] else [columns[0]],
        "surrogate_key": f"{table_name}_sk",
        "effective_from": "effective_from",
        "effective_to": "effective_to",
        "current_flag": "is_current",
        "target_integration": {"table": f"int_{table_name}_stage", "mappings": mappings},
        "target_dwh": {"table": f"dw_{table_name}"}
    }
    return sttm
