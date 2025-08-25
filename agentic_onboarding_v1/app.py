
import os, io, json, time, yaml, pandas as pd
import streamlit as st
from pathlib import Path

from agents.orchestrator import route_intent
from agents import landing_agent, dq_agent, integration_agent, dwh_agent, reporting_agent
from tools.utils import step_logger, run_id as new_run_id, load_yaml
from tools.connectors import write_sqlite, read_sqlite, write_local_csv, read_local_csv
from tools.sttm import load_sttm, generate_sttm_from_brd
from tools.transforms import cast_types

st.set_page_config(page_title="Agentic Data Onboarding Chatbot", page_icon="🧠", layout="wide")

# ---- Sidebar: Quickstart & Connections ----
with st.sidebar:
    st.header("⚙️ Quickstart & Connections")
    if st.button("🚀 Quickstart Demo (Local CSV → SQLite)"):
        st.session_state["quickstart"] = True
    st.caption("Optional extras are lazy-loaded; local demo works without cloud creds.")
    st.divider()
    st.subheader("Local SQLite paths")
    int_db = st.text_input("Integration DB", value="data/integration.db")
    dwh_db = st.text_input("Warehouse DB", value="data/warehouse.db")
    st.session_state["integration_db"] = int_db
    st.session_state["warehouse_db"] = dwh_db
    st.divider()
    st.subheader("Email (optional)")
    st.text_input("SMTP Host", key="smtp_host")
    st.text_input("SMTP Port", key="smtp_port", value="587")
    st.text_input("SMTP User", key="smtp_user")
    st.text_input("SMTP Password", key="smtp_pass", type="password")
    st.text_input("From Email", key="from_email")
    st.text_input("To Email", key="to_email")

st.title("🤖 Agentic Data Onboarding Chatbot")
st.write("I can help with **Landing → Data Quality → Integration → Data Warehouse → Reporting**. Ask me in natural language, or use the Quickstart.")

if "chat" not in st.session_state:
    st.session_state.chat = []
if "run_id" not in st.session_state:
    st.session_state.run_id = new_run_id()
if "state" not in st.session_state:
    st.session_state.state = {
        "landing": {}, "dq": {}, "integration": {}, "dwh": {}, "report": {}
    }

def log_to_ui(msg: str, kind="assistant"):
    st.chat_message(kind).write(msg)

def think(msg: str, secs: float = 0.7):
    log_to_ui(f"_{msg}_")
    time.sleep(secs)

# ---- Quickstart path ----
def quickstart_pipeline():
    log_to_ui("👋 Welcome! Let's onboard sample data end-to-end.")
    run_id = st.session_state.run_id
    int_db = st.session_state["integration_db"]
    dwh_db = st.session_state["warehouse_db"]
    with step_logger(log_to_ui, "Landing: read & profile sample customers"):
        uri = "file://data/samples/customers.csv"
        df = read_local_csv("data/samples/customers.csv")
        st.session_state.state["landing"] = {"source": uri, "rows": len(df)}
        think("profiling columns…")
        from tools import dq as dqtools
        prof = dqtools.profile(df)
        st.session_state.state["landing"]["profile"] = prof
        props = {"birthdate": "date"}
        think("proposing type fixes…")
        df = cast_types(df, props)
        write_sqlite(df, int_db, "landing_customers", if_exists="replace")
        st.session_state.state["landing"]["landed_table"] = "landing_customers"
    with step_logger(log_to_ui, "DQ: generate & apply rules"):
        rules = dq_agent.generate_rules(df)
        # add example membership rule
        rules.append({"type":"set_membership","column":"country","allowed":["US","CA","UK"]})
        _, results = dq_agent.run_rules(df, rules)
        st.session_state.state["dq"] = {"rules": rules, "results": results}
        bad = [r for r in results if not r["passed"]]
        if bad:
            log_to_ui(f"⚠️ DQ issues detected: {len(bad)} rule(s) failed. You can refine rules in chat.")
    with step_logger(log_to_ui, "Integration: STTM transform & SCD2 load"):
        sttm = load_yaml("sttm/customer_dim_sttm.yaml")
        out = integration_agent.transform_to_integration(df, sttm)
        # Read current stage if exists
        try:
            existing = read_sqlite(int_db, "SELECT * FROM int_customer_dim_stage")
        except Exception:
            existing = None
        merged = integration_agent.load_integration(existing, out, sttm)
        write_sqlite(merged, int_db, "int_customer_dim_stage", if_exists="replace")
        st.session_state.state["integration"] = {"table":"int_customer_dim_stage","rows": len(merged)}
    with step_logger(log_to_ui, "DWH: filter US/CA and SCD2 merge"):
        from agents.dwh_agent import to_dwh, load_dwh
        dwh_in = to_dwh(merged, sttm)
        # Read current DWH if exists
        try:
            existing = read_sqlite(dwh_db, "SELECT * FROM dw_customer_dim")
        except Exception:
            existing = None
        dwh_merged = load_dwh(existing, dwh_in, sttm)
        write_sqlite(dwh_merged, dwh_db, "dw_customer_dim", if_exists="replace")
        st.session_state.state["dwh"] = {"table":"dw_customer_dim","rows": len(dwh_merged)}
    with step_logger(log_to_ui, "Reporting: compile run report"):
        ctx = {"run_id": run_id, **st.session_state.state}
        report_path = f"reports/{run_id}.html"
        reporting_agent.write_html_report(report_path, ctx)
        st.session_state.state["report"] = {"report": report_path}
        st.success(f"Report ready: {report_path}")

# Show quickstart button result
if st.session_state.get("quickstart"):
    quickstart_pipeline()

# ---- Chat UI ----
for role, content in st.session_state.chat:
    st.chat_message(role).write(content)

prompt = st.chat_input("Tell me what to do (e.g., 'Profile this CSV and load to DWH as SCD2')")
if prompt:
    st.session_state.chat.append(("user", prompt))
    st.chat_message("user").write(prompt)

    intent = route_intent(prompt)
    st.session_state.chat.append(("assistant", f"thinking about your request… (intent='{intent}')"))
    log_to_ui(f"thinking about your request… (intent='{intent}')")

    # Minimal interactive behaviors per intent (demo-ready)
    if intent == "landing":
        with st.chat_message("assistant"):
            st.write("🛬 Let's start with Landing. Provide a source URI or upload a CSV.")
            up = st.file_uploader("Upload CSV", type=["csv"], key=f"up_{len(st.session_state.chat)}")
            uri = st.text_input("…or enter URI (file:// or s3://)", value="file://data/samples/customers.csv", key=f"uri_{len(st.session_state.chat)}")
            if st.button("Run Landing", key=f"run_land_{len(st.session_state.chat)}"):
                df = read_local_csv(uri.replace("file://","")) if uri.startswith("file://") else None
                with st.spinner("profiling…"):
                    from tools import dq as dqtools
                    prof = dqtools.profile(df)
                    st.write(prof)
                st.session_state.state["landing"] = {"source": uri, "profile": prof, "rows": len(df)}

    elif intent == "dq":
        with st.chat_message("assistant"):
            st.write("🧪 DQ: I can generate rules from the Landing data and run them.")
            int_db = st.session_state["integration_db"]
            try:
                df = read_sqlite(int_db, "SELECT * FROM landing_customers")
            except Exception:
                df = read_local_csv("data/samples/customers.csv")
            rules = dq_agent.generate_rules(df)
            st.write("Proposed rules:", rules)
            if st.button("Run DQ now"):
                _, res = dq_agent.run_rules(df, rules)
                st.write(res)
                st.session_state.state["dq"] = {"rules": rules, "results": res}

    elif intent == "integration":
        with st.chat_message("assistant"):
            st.write("🔁 Integration: supply STTM (YAML) and SCD type. I'll transform and stage.")
            sttm_path = st.text_input("STTM path", value="sttm/customer_dim_sttm.yaml")
            if st.button("Run Integration"):
                df = read_local_csv("data/samples/customers.csv")
                sttm = load_yaml(sttm_path)
                out = integration_agent.transform_to_integration(df, sttm)
                try:
                    existing = read_sqlite(st.session_state["integration_db"], f"SELECT * FROM {sttm['target_integration']['table']}")
                except Exception:
                    existing = None
                merged = integration_agent.load_integration(existing, out, sttm)
                write_sqlite(merged, st.session_state["integration_db"], sttm['target_integration']['table'], if_exists="replace")
                st.write("Loaded rows:", len(merged))

    elif intent == "dwh":
        with st.chat_message("assistant"):
            st.write("🏛️ DWH: I'll apply filters and load with SCD to warehouse tables.")
            sttm_path = st.text_input("STTM path", value="sttm/customer_dim_sttm.yaml")
            if st.button("Run DWH"):
                sttm = load_yaml(sttm_path)
                from sqlalchemy import text
                try:
                    integ = read_sqlite(st.session_state["integration_db"], f"SELECT * FROM {sttm['target_integration']['table']}")
                except Exception as e:
                    st.error("No integration table found. Run Integration first or use Quickstart.")
                    st.stop()
                from agents.dwh_agent import to_dwh, load_dwh
                dwh_in = to_dwh(integ, sttm)
                try:
                    existing = read_sqlite(st.session_state["warehouse_db"], f"SELECT * FROM {sttm['target_dwh']['table']}")
                except Exception:
                    existing = None
                merged = load_dwh(existing, dwh_in, sttm)
                write_sqlite(merged, st.session_state["warehouse_db"], sttm['target_dwh']['table'], if_exists="replace")
                st.write("Loaded rows:", len(merged))

    elif intent == "report":
        with st.chat_message("assistant"):
            run_id = st.session_state.run_id
            ctx = {"run_id": run_id, **st.session_state.state}
            path = f"reports/{run_id}.html"
            reporting_agent.write_html_report(path, ctx)
            st.success(f"Report generated at {path}")

    else:
        log_to_ui("I’ll begin with Landing. You can then ask me to run DQ, Integration, DWH, and Reporting.")
