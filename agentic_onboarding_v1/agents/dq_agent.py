
from typing import Dict, Any, List, Tuple
import pandas as pd
from ..tools import dq as dqtools

def generate_rules(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return dqtools.propose_rules(df)

def run_rules(df: pd.DataFrame, rules: List[Dict[str, Any]]):
    return dqtools.apply_rules(df, rules)
