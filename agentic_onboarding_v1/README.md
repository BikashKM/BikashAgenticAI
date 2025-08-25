# Agentic Data Onboarding Chatbot (v1)

An interactive, ChatGPT-style **multi-agent** chatbot for end-to-end **data onboarding** with five backend agents:
**Landing, Data Quality, Integration, Data Warehouse, Reporting**. Works locally out-of-the-box with **Local CSV → SQLite**.
Optional connectors for **S3, Databricks, Redshift, Snowflake** are included (enable with credentials).

## Quickstart

```bash
python -m venv venv && source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
streamlit run app.py
```

The app launches a chat UI. Use the **Quickstart Demo** button to run end-to-end with sample data.

## Features
- Parent **Orchestrator** routes intents to specialist agents based on natural language.
- **ChatGPT-style** streaming UX with “thinking…”, step chips, and live logs.
- **Landing**: ingest + profile + type proposals.
- **Data Quality**: auto-generate/edit/apply rules (not_null/unique/range/regex/set_membership/freshness).
- **Integration**: transform Landing → Integration via **STTM** + **SCD1/2** (auto-generate STTM from BRD if missing).
- **Data Warehouse**: Integration → DWH with SCD + conversational filters.
- **Reporting**: compile run status into an HTML report; optional email via SMTP/SES.
- Any-to-any connectors (Local CSV, S3, Databricks, Redshift, Snowflake). Local **SQLite** works out-of-the-box.

## Configuration
- Place secrets in environment variables or `config/connections.example.yaml` → copy to `connections.yaml`.
- Optional OpenAI key to enhance STTM / rule generation: `OPENAI_API_KEY`.

## Folder Layout
```
.
├── app.py                  # Streamlit chat app (main)
├── requirements.txt
├── README.md
├── agents/
│   ├── orchestrator.py
│   ├── landing_agent.py
│   ├── dq_agent.py
│   ├── integration_agent.py
│   ├── dwh_agent.py
│   └── reporting_agent.py
├── tools/
│   ├── connectors.py
│   ├── dq.py
│   ├── sttm.py
│   ├── transforms.py
│   └── utils.py
├── data/samples/           # sample CSVs
├── sttm/                   # sample STTM YAML
└── reports/                # generated run reports
```

## Connectors (Optional)
- **Local CSV**: No config required.
- **S3**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`.
- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`.
- **Redshift**: `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DB`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`.
- **Databricks**: `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`.

> Connectors import lazily. If libs are missing or creds absent, the UI will explain and continue with local demo.

## Sample Data
- `data/samples/customers.csv`
- `data/samples/orders.csv`

## Sample STTM
- `sttm/customer_dim_sttm.yaml`
- `sttm/sales_fact_sttm.yaml`

## Notes
- SQLite database files are created in `./data/` (integration.db, warehouse.db) during runs.
- For email, set SMTP or SES env vars; otherwise the app saves the HTML report and shows a download link.

Enjoy!
