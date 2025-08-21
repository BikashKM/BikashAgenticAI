# Agentic ETL — Full Package v1.5.1 (Excel STTM, fixed keys & conditional connectors)

This package includes:
- Excel STTM driving Integration & DWH
- Native MERGE + S3 COPY for Snowflake/Redshift/Databricks
- DQ rules, Reporting (HTML + optional PDF)
- **Unique Streamlit widget keys** (fix duplicate key error)
- **Conditional connector config** (S3/Snowflake/Redshift/Databricks expanders appear only if selected as targets or source=S3)
- Samples, Batch Runner, NL Command Center

## Quickstart
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```
