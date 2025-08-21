#!/usr/bin/env python3
import os, re, sys

ROOT = os.path.dirname(__file__)
APP = os.path.join(ROOT, "app.py")
if not os.path.exists(APP):
    print("ERROR: app.py not found in this folder. Run this script from your project root (where app.py lives).")
    sys.exit(1)

with open(APP, "r", encoding="utf-8") as f:
    code = f.read()

# Integration: make SCD/keys editable with defaults from STTM
integration_pattern = r"scd_type = get_scd_for_target\(sttm_df, \"integration\", target_choice, default_scd=state.get\(\"scd_type\",\"SCD1\"\)\)\n\s+keys = get_keys_for_target\(sttm_df, \"integration\", target_choice\)"
integration_repl = (
    "scd_type = get_scd_for_target(sttm_df, \"integration\", target_choice, default_scd=state.get(\"scd_type\",\"SCD1\"))\n"
    "            keys = get_keys_for_target(sttm_df, \"integration\", target_choice)\n"
    "            # Editable defaults from STTM\n"
    "            scd_type = st.selectbox(\"SCD type (from STTM, editable)\", [\"SCD1\",\"SCD2\",\"SCD3\"], "
    "index=[\"SCD1\",\"SCD2\",\"SCD3\"].index(scd_type if scd_type in [\"SCD1\",\"SCD2\",\"SCD3\"] else \"SCD1\"), key=\"integ_scd_select\")\n"
    "            keys_text = st.text_input(\"Business key columns (from STTM, editable)\", \",\".join(keys), key=\"integ_keys_input\")\n"
    "            keys = [c.strip() for c in keys_text.split(\",\") if c.strip()]"
)
code, n1 = re.subn(integration_pattern, integration_repl, code)

# DWH: make SCD/keys editable with defaults from STTM
dwh_pattern = r"scd_type = get_scd_for_target\(sttm_df, \"dwh\", dwh_table, default_scd=\"SCD1\"\)\n\s+keys = get_keys_for_target\(sttm_df, \"dwh\", dwh_table\)"
dwh_repl = (
    "scd_type = get_scd_for_target(sttm_df, \"dwh\", dwh_table, default_scd=\"SCD1\")\n"
    "            keys = get_keys_for_target(sttm_df, \"dwh\", dwh_table)\n"
    "            # Editable defaults from STTM\n"
    "            scd_type = st.selectbox(\"SCD type (from STTM, editable)\", [\"SCD1\",\"SCD2\",\"SCD3\"], "
    "index=[\"SCD1\",\"SCD2\",\"SCD3\"].index(scd_type if scd_type in [\"SCD1\",\"SCD2\",\"SCD3\"] else \"SCD1\"), key=\"dwh_scd_select\")\n"
    "            keys_text = st.text_input(\"Business key columns (from STTM, editable)\", \",\".join(keys), key=\"dwh_keys_input\")\n"
    "            keys = [c.strip() for c in keys_text.split(\",\") if c.strip()]"
)
code, n2 = re.subn(dwh_pattern, dwh_repl, code)

with open(APP, "w", encoding="utf-8") as f:
    f.write(code)

print(f"Patched app.py. Integration edits: {n1}, DWH edits: {n2}")
