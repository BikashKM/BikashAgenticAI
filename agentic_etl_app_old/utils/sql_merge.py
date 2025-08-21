from __future__ import annotations
from typing import List

def quote_ident(name: str, dialect: str) -> str:
    if dialect == "databricks":
        return f"`{name}`"
    return f'"{name}"'

def build_merge_sql(dialect: str, target: str, staging: str, keys: List[str], columns: List[str]) -> str:
    q = lambda n: quote_ident(n, dialect)
    on_clause = " AND ".join([f"t.{q(k)} = s.{q(k)}" for k in keys])
    non_keys = [c for c in columns if c not in keys]
    set_clause = ", ".join([f"{q(c)} = s.{q(c)}" for c in non_keys]) if non_keys else f"{q(keys[0])} = s.{q(keys[0])}"
    insert_cols = ", ".join([q(c) for c in columns])
    insert_vals = ", ".join([f"s.{q(c)}" for c in columns])
    return f"""MERGE INTO {target} t
USING {staging} s
ON {on_clause}
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
"""
