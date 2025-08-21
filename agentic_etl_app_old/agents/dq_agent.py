from __future__ import annotations
from typing import Optional, List, Dict, Any
import pandas as pd
from utils import dq_rules as R

class DQAgent:
    def __init__(self, use_llm: bool = False):
        self.use_llm = use_llm

    def propose_rules(self, df: pd.DataFrame, primary_keys: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return R.propose_rules(df, primary_keys)

    def run_checks(self, df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        return R.run_checks(df, rules)
