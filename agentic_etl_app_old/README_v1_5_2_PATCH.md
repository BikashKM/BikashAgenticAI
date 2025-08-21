# Agentic ETL v1.5.2 Patch — Bytes-safe state + STTM defaults in UI

This patch fixes:
1) **TypeError: Object of type bytes is not JSON serializable** — the state store now base64-encodes bytes.
2) **Auto-detected SCD/keys** from the Excel STTM are now **pre-filled in the UI** and editable.

## Apply
Unzip this over your v1.5.1 project root (where `app.py` is), then run:
```bash
python patch_app_sttm_defaults.py
```
You should see: `Patched app.py. Integration edits: 1, DWH edits: 1`

That's it — start the app:
```bash
streamlit run app.py
```

## What changed
- `utils/session_store.py`:
  - Bytes are saved as JSON-friendly base64 blobs and restored on load.
- `app.py` (patched in-place by the script):
  - In **Integration** and **DWH** STTM sections, SCD type and business keys are read from STTM and shown as **editable defaults** in the UI:
    - `SCD type (from STTM, editable)`
    - `Business key columns (from STTM, editable)`
  - Each has a unique widget key to avoid `DuplicateWidgetID`.
