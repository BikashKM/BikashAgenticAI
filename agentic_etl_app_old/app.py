from __future__ import annotations
import streamlit as st, pandas as pd, os, io, json, uuid, datetime as dt
from utils.session_store import load_state, save_state
from connectors.local_csv import write_df as write_local_df, read_local_csv
from connectors.s3_connector import S3Connector
from connectors.snowflake_connector import SnowflakeConnector
from connectors.redshift_connector import RedshiftConnector
from connectors.databricks_connector import DatabricksConnector
from agents.dq_agent import DQAgent
from agents.transform_agent import TransformAgent
from agents.report_agent import render_report_html, save_html_and_pdf
from utils.emailer import send_email_smtp, send_email_ses
from utils.sttm_parser import load_sttm_excel, build_integration_plan, project_columns, get_scd_for_target, get_keys_for_target, build_dwh_targets

st.set_page_config(page_title="Agentic AI ETL", layout="wide")
st.title("🤖 Agentic AI ETL – Interactive POC")

state = load_state()
if "run_id" not in state:
    state["run_id"] = uuid.uuid4().hex[:8]

with st.sidebar:
    st.header("Navigate")
    step = st.radio("Go to step:", ["1) Configure", "2) Landing Load", "3) DQ Rules", "4) Integration", "5) DWH", "6) Reporting", "7) Batch Runner"], index=0, key="nav_step")

# ----------------------------- Step 1 -----------------------------
if step.startswith("1"):
    st.subheader("1) Configure")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Source**")
        source_type = st.selectbox("Source Type", ["Local CSV","S3"], index=["Local CSV","S3"].index(state.get("source_type","Local CSV")), key="cfg_source_type")
        state["source_type"] = source_type
        if source_type == "Local CSV":
            up = st.file_uploader("Upload a CSV", type="csv", key="cfg_upload_csv")
            if up:
                state["dataset_name"] = os.path.splitext(up.name)[0]
                state["uploaded_csv_bytes"] = up.read()
        else:
            s3_path = st.text_input("S3 URI (s3://bucket/key.csv)", value=state.get("source_s3_uri",""), key="cfg_source_s3_uri")
            state["source_s3_uri"] = s3_path
    with c2:
        st.markdown("**Targets**")
        state["landing_target"] = st.selectbox("Landing Target", ["Local CSV","S3","Snowflake","Redshift","Databricks"], index=["Local CSV","S3","Snowflake","Redshift","Databricks"].index(state.get("landing_target","Local CSV")), key="cfg_landing_target")
        state["integration_target"] = st.selectbox("Integration Target", ["Local CSV","S3","Snowflake","Redshift","Databricks"], index=["Local CSV","S3","Snowflake","Redshift","Databricks"].index(state.get("integration_target","Local CSV")), key="cfg_integration_target")
        state["dwh_target"] = st.selectbox("DWH Target", ["Local CSV","S3","Snowflake","Redshift","Databricks"], index=["Local CSV","S3","Snowflake","Redshift","Databricks"].index(state.get("dwh_target","Local CSV")), key="cfg_dwh_target")

    selected_targets = {state.get("landing_target"), state.get("integration_target"), state.get("dwh_target")}

    if "S3" in selected_targets or source_type == "S3":
        with st.expander("S3 Settings", expanded=True):
            state["s3_region"] = st.text_input("AWS Region", value=state.get("s3_region","us-east-1"), key="cfg_s3_region")
            state["s3_bucket"] = st.text_input("Default S3 bucket", value=state.get("s3_bucket",""), key="cfg_s3_bucket")
            state["s3_prefix"] = st.text_input("Default prefix", value=state.get("s3_prefix","agentic-etl"), key="cfg_s3_prefix")
            st.caption("S3 subfolders: landing/, integration/, dwh/ under the prefix.")
            st.markdown("**(Optional for DB COPY)**")
            state["aws_key"] = st.text_input("AWS Access Key ID", value=state.get("aws_key",""), key="cfg_aws_key")
            state["aws_secret"] = st.text_input("AWS Secret Access Key", value=state.get("aws_secret",""), type="password", key="cfg_aws_secret")

    if "Snowflake" in selected_targets:
        with st.expander("Snowflake", expanded=True):
            state["sf_user"] = st.text_input("User", value=state.get("sf_user",""), key="cfg_sf_user")
            state["sf_password"] = st.text_input("Password", value=state.get("sf_password",""), type="password", key="cfg_sf_password")
            state["sf_account"] = st.text_input("Account (xyz-xx)", value=state.get("sf_account",""), key="cfg_sf_account")
            state["sf_warehouse"] = st.text_input("Warehouse", value=state.get("sf_warehouse",""), key="cfg_sf_wh")
            state["sf_database"] = st.text_input("Database", value=state.get("sf_database",""), key="cfg_sf_db")
            state["sf_schema"] = st.text_input("Schema", value=state.get("sf_schema",""), key="cfg_sf_schema")
            state["sf_role"] = st.text_input("Role (optional)", value=state.get("sf_role",""), key="cfg_sf_role")

    if "Redshift" in selected_targets:
        with st.expander("Redshift", expanded=True):
            state["rs_host"] = st.text_input("Host", value=state.get("rs_host",""), key="cfg_rs_host")
            state["rs_port"] = st.text_input("Port", value=str(state.get("rs_port","5439")), key="cfg_rs_port")
            state["rs_db"] = st.text_input("Database", value=state.get("rs_db",""), key="cfg_rs_db")
            state["rs_user"] = st.text_input("User", value=state.get("rs_user",""), key="cfg_rs_user")
            state["rs_password"] = st.text_input("Password", value=state.get("rs_password",""), type="password", key="cfg_rs_password")

    if "Databricks" in selected_targets:
        with st.expander("Databricks (SQL Warehouse)", expanded=True):
            state["db_host"] = st.text_input("Host", value=state.get("db_host",""), key="cfg_db_host")
            state["db_http"] = st.text_input("HTTP Path", value=state.get("db_http",""), key="cfg_db_http")
            state["db_token"] = st.text_input("Token", value=state.get("db_token",""), type="password", key="cfg_db_token")
            state["db_catalog"] = st.text_input("Catalog (optional)", value=state.get("db_catalog",""), key="cfg_db_catalog")
            state["db_schema"] = st.text_input("Schema (optional)", value=state.get("db_schema",""), key="cfg_db_schema")

    with st.expander("Email (SMTP/SES)", expanded=False):
        state["email_sender"] = st.text_input("Sender email", value=state.get("email_sender",""), key="cfg_email_sender")
        state["email_to"] = st.text_input("Recipient emails (comma)", value=state.get("email_to",""), key="cfg_email_to")
        state["email_method"] = st.selectbox("Method", ["SMTP","SES"], index=["SMTP","SES"].index(state.get("email_method","SMTP")), key="cfg_email_method")
        state["smtp_host"] = st.text_input("SMTP host", value=state.get("smtp_host","smtp.gmail.com"), key="cfg_smtp_host")
        state["smtp_port"] = st.number_input("SMTP port", value=int(state.get("smtp_port",587)), key="cfg_smtp_port")
        state["smtp_user"] = st.text_input("SMTP username", value=state.get("smtp_user",""), key="cfg_smtp_user")
        state["smtp_pass"] = st.text_input("SMTP password", value=state.get("smtp_pass",""), type="password", key="cfg_smtp_pass")
        state["ses_region"] = st.text_input("SES region", value=state.get("ses_region","us-east-1"), key="cfg_ses_region")

    st.markdown("> **Tip:** Use the **Command Center** below to run the whole pipeline using plain English.")
    with st.expander("⚡ Command Center (Natural Language)", expanded=False):
        st.code("Load the sample customers and orders from local CSV, run DQ with default rules, use SCD2 for customers and SCD1 for orders, then load integration and DWH locally and email me the report.", language="text")
        nl = st.text_area("Your instruction", key="cmd_text")
        if st.button("Run by instruction", key="cmd_run"):
            intent = {
                "use_samples": any(k in nl.lower() for k in ["sample","customers","orders"]),
                "targets": "local" if "local" in nl.lower() else ("snowflake" if "snowflake" in nl.lower() else ("redshift" if "redshift" in nl.lower() else ("databricks" if "databricks" in nl.lower() else "local"))),
                "email": any(k in nl.lower() for k in ["email","mail"]),
            }
            st.write("Interpreted intent:", intent)
            customers_df = pd.read_csv("data/samples/customers.csv")
            orders_df = pd.read_csv("data/samples/orders.csv")
            state["dataset_name"] = "customers"; state["uploaded_csv_bytes"] = customers_df.to_csv(index=False).encode("utf-8")
            state["source_type"] = "Local CSV"
            tmap = {"local":"Local CSV","snowflake":"Snowflake","redshift":"Redshift","databricks":"Databricks"}
            state["landing_target"] = tmap[intent["targets"]]; state["integration_target"] = tmap[intent["targets"]]; state["dwh_target"] = tmap[intent["targets"]]
            path = write_local_df(customers_df, "landing", "customers")
            state["landing_info"] = {"records": len(customers_df), "columns": list(customers_df.columns), "location": path}
            state["landing_df_csv"] = customers_df.to_csv(index=False)
            dq = DQAgent(); rules = dq.propose_rules(customers_df, ["customer_id"]); report = dq.run_checks(customers_df, rules)
            state["dq_rules"]=rules; state["dq_report"]=report; state["dq_override"]=True
            save_state(state); st.success("Prepped samples. Proceed to Integration and DWH using STTM, then Reporting.")

    save_state(state)
    st.success("Configuration saved.")

# ----------------------------- Step 2 -----------------------------
elif step.startswith("2"):
    st.subheader("2) Landing Load")
    if state.get("source_type","Local CSV") == "Local CSV":
        if "uploaded_csv_bytes" not in state:
            st.warning("Upload a CSV in Configure step or use Command Center to load samples.")
        else:
            df = pd.read_csv(io.BytesIO(state["uploaded_csv_bytes"]))
            st.dataframe(df.head(20))
            name = st.text_input("Landing dataset name", value=state.get("dataset_name","dataset"), key="land_name")
            target = state.get("landing_target","Local CSV")
            rows_written = 0; location = ""
            if st.button("Write Landing", key="land_write"):
                if target == "Local CSV":
                    location = write_local_df(df, "landing", name)
                    rows_written = len(df)
                elif target == "S3":
                    s3 = S3Connector(region=state.get("s3_region","us-east-1"))
                    uri = f"s3://{state.get('s3_bucket')}/{state.get('s3_prefix','agentic-etl')}/landing/{name}.csv"
                    s3.write_csv(df, uri); location = uri; rows_written = len(df)
                else:
                    st.info("For DB landing, integration writes will create tables; landing as Local/S3 is sufficient for POC.")
                state["landing_info"] = {"records": rows_written, "columns": list(df.columns), "location": location}
                state["landing_df_csv"] = df.to_csv(index=False)
                save_state(state)
                st.success(f"Landing written: {rows_written} rows → {location}")
    else:
        s3_uri = state.get("source_s3_uri","")
        if st.button("Load from S3", key="land_load_s3"):
            s3 = S3Connector(region=state.get("s3_region","us-east-1"))
            df = s3.read_csv(s3_uri)
            st.dataframe(df.head(20))
            state["dataset_name"] = os.path.splitext(os.path.basename(s3_uri))[0]
            state["landing_df_csv"] = df.to_csv(index=False)
            state["landing_info"] = {"records": len(df), "columns": list(df.columns), "location": s3_uri}
            save_state(state)

# ----------------------------- Step 3 -----------------------------
elif step.startswith("3"):
    st.subheader("3) DQ Rules")
    if "landing_df_csv" not in state:
        st.warning("Run Landing first.")
    else:
        df = pd.read_csv(io.StringIO(state["landing_df_csv"]))
        st.dataframe(df.head(20))
        default_pk = [c for c in df.columns if c.lower()=="id" or c.lower().endswith("_id")]
        pk_text = st.text_input("Primary keys (comma)", value=",".join(default_pk) if default_pk else "", key="dq_pk")
        pks = [c.strip() for c in pk_text.split(",") if c.strip()]
        dq = DQAgent()
        if st.button("Suggest Rules", key="dq_suggest"):
            rules = dq.propose_rules(df, pks)
            state["dq_rules"] = rules; save_state(state)
        rules = state.get("dq_rules", [])
        if rules:
            st.json(rules)
            if st.button("Run DQ", key="dq_run"):
                report = dq.run_checks(df, rules)
                st.json(report["summary"])
                state["dq_report"] = report; save_state(state)
        if state.get("dq_report"):
            crit_fail = any((r["severity"]=="CRITICAL" and not r["passed"]) for r in state["dq_report"]["results"])
            approve = st.checkbox("Approve to proceed despite CRITICAL failures", value=state.get("dq_override", not crit_fail), key="dq_approve")
            state["dq_override"] = bool(approve); save_state(state)

# ----------------------------- Step 4 -----------------------------
elif step.startswith("4"):
    st.subheader("4) Integration Layer")
    if "landing_df_csv" not in state or not state.get("dq_override", False):
        st.warning("Need Landing data and DQ approval first."); st.stop()
    landing_df = pd.read_csv(io.StringIO(state["landing_df_csv"]))
    st.write("Landing sample:"); st.dataframe(landing_df.head(20))

    use_sttm = st.checkbox("Use Excel STTM (sttm/STTM_template.xlsx) for integration", value=True, key="int_use_sttm")
    st.markdown("#### Reference / Dimension Data")
    ref_df_map = {}
    if use_sttm:
        try:
            sttm_df = load_sttm_excel(os.path.join("sttm","STTM_template.xlsx"))
            integ_targets = sorted(sttm_df[sttm_df["Target Schema"].str.lower()=="integration"]["Target Table"].unique().tolist())
            target_choice = st.selectbox("Pick Integration target (from STTM)", integ_targets, index=0, key="int_target_choice")
            plan = build_integration_plan(sttm_df, target_choice)
            for r in plan["refs"]:
                name = r["name"]
                csv_path = os.path.join("data","samples", f"{name}.csv")
                if os.path.exists(csv_path):
                    ref_df_map[name] = pd.read_csv(csv_path)
            transformer = TransformAgent()
            integrated_tmp = transformer.integrate(landing_df, ref_df_map, {"left_on": plan["left_on"], "refs": plan["refs"]})
            df_map = {("landing", state.get("dataset_name","landing_dataset")): integrated_tmp}
            for r in plan["refs"]:
                name = r["name"]
                if name in ref_df_map:
                    df_map[("reference", name)] = ref_df_map[name]
            integrated = project_columns(df_map, plan["projection"])
            scd_type = get_scd_for_target(sttm_df, "integration", target_choice, default_scd=state.get("scd_type","SCD1"))
            keys = get_keys_for_target(sttm_df, "integration", target_choice)
            # Editable defaults from STTM
            scd_type = st.selectbox("SCD type (from STTM, editable)", ["SCD1","SCD2","SCD3"], index=["SCD1","SCD2","SCD3"].index(scd_type if scd_type in ["SCD1","SCD2","SCD3"] else "SCD1"), key="integ_scd_select")
            keys_text = st.text_input("Business key columns (from STTM, editable)", ",".join(keys), key="integ_keys_input")
            keys = [c.strip() for c in keys_text.split(",") if c.strip()]
            st.info(f"STTM plan → target={target_choice}, scd={scd_type}, keys={keys}, left_on={plan['left_on']}")
            table_name = target_choice
        except Exception as e:
            st.error(f"Failed to use STTM: {e}"); use_sttm=False
    if not use_sttm:
        st.warning("Manual JSON join path not implemented in this build; enable STTM.")
        st.stop()

    use_merge = st.checkbox("Use native MERGE (where supported)", value=True, key="int_merge")
    use_s3_copy = st.checkbox("Prefer S3 COPY for bulk load (Snowflake/Redshift/Databricks)", value=False, key="int_copy")
    copy_prefix = st.text_input("S3 COPY prefix", value=state.get("copy_prefix", f"s3://{state.get('s3_bucket','')}/{state.get('s3_prefix','agentic-etl')}/integration"), key="int_copy_prefix")
    state["copy_prefix"] = copy_prefix

    if st.button("Run Integration", key="int_run"):
        target = state.get("integration_target","Local CSV")
        rows_written = 0; location = ""
        if target == "Local CSV":
            existing = None
            path_exist = os.path.join("data","integration",f"{table_name}.csv")
            if os.path.exists(path_exist):
                existing = pd.read_csv(path_exist)
            merged = TransformAgent().scd_load(existing, integrated, scd_type, keys or [])
            location = write_local_df(merged, "integration", table_name); rows_written = len(merged)
        elif target == "S3":
            s3 = S3Connector(region=state.get("s3_region","us-east-1"))
            uri = f"s3://{state.get('s3_bucket')}/{state.get('s3_prefix','agentic-etl')}/integration/{table_name}.csv"
            s3.write_csv(integrated, uri); location = uri; rows_written = len(integrated)
        elif target == "Snowflake":
            sf = SnowflakeConnector(state.get("sf_user",""), state.get("sf_password",""), state.get("sf_account",""), state.get("sf_warehouse",""), state.get("sf_database",""), state.get("sf_schema",""), state.get("sf_role") or None)
            if use_s3_copy:
                from connectors.s3_connector import S3Connector as _S3; s3 = _S3(region=state.get("s3_region","us-east-1"))
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; s3.write_csv(integrated, copy_uri)
                cols = ", ".join([f'"{c}" VARCHAR' for c in integrated.columns]); sf.execute(f'create table if not exists {table_name} ({cols})')
                sf.copy_from_s3(table_name, copy_uri, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"))
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; sf.execute(f'create or replace table {stg} as select * from {table_name} where 1=2')
                    sf.copy_from_s3(stg, copy_uri, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"))
                    from utils.sql_merge import build_merge_sql; sf.execute(build_merge_sql("snowflake", table_name, stg, keys, list(integrated.columns))); sf.execute(f'drop table if exists {stg}')
                location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"; rows_written = len(integrated)
            elif use_merge and keys:
                rows_written = sf.merge_df(integrated, target_table=table_name, keys=keys); location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"
            else:
                rows_written = sf.write_df(integrated, table=table_name, overwrite=False); location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"
        elif target == "Redshift":
            rs = RedshiftConnector(state.get("rs_host",""), int(state.get("rs_port","5439")), state.get("rs_db",""), state.get("rs_user",""), state.get("rs_password",""), ssl=True)
            if use_s3_copy:
                from connectors.s3_connector import S3Connector as _S3; s3 = _S3(region=state.get("s3_region","us-east-1"))
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; s3.write_csv(integrated, copy_uri)
                cols = ", ".join([f'"{c}" varchar' for c in integrated.columns]); rs.execute(f'create table if not exists {table_name} ({cols});')
                rs.copy_from_s3(table_name, copy_uri, iam_role=None, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"), region=state.get("s3_region"))
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; rs.execute(f'drop table if exists {stg}; create table {stg} ({cols});')
                    rs.copy_from_s3(stg, copy_uri, iam_role=None, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"), region=state.get("s3_region"))
                    from utils.sql_merge import build_merge_sql; rs.execute(build_merge_sql("redshift", table_name, stg, keys, list(integrated.columns))); rs.execute(f'drop table if exists {stg}')
                location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"; rows_written = len(integrated)
            elif use_merge and keys:
                stg = f"{table_name}__stg"; cols = ", ".join([f'"{c}" varchar' for c in integrated.columns]); rs.execute(f'drop table if exists {stg}; create table {stg} ({cols});')
                rs.write_df(integrated, table=stg, overwrite=False); from utils.sql_merge import build_merge_sql
                rs.execute(f'create table if not exists {table_name} ({cols});'); rs.execute(build_merge_sql("redshift", table_name, stg, keys, list(integrated.columns))); rs.execute(f'drop table if exists {stg}')
                location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"; rows_written = len(integrated)
            else:
                rows_written = rs.write_df(integrated, table=table_name, overwrite=False); location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"
        elif target == "Databricks":
            dbc = DatabricksConnector(state.get("db_host",""), state.get("db_http",""), state.get("db_token",""), state.get("db_catalog") or None, state.get("db_schema") or None)
            if use_s3_copy:
                cols = ", ".join([f"`{c}` string" for c in integrated.columns]); dbc.execute(f"create table if not exists {dbc.qualified(table_name)} ({cols}) using delta")
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; from connectors.s3_connector import S3Connector as _S3; _S3(state.get("s3_region","us-east-1")).write_csv(integrated, copy_uri)
                dbc.copy_into_from_s3(table_name, copy_uri)
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; dbc.execute(f"drop table if exists {dbc.qualified(stg)}"); dbc.execute(f"create table {dbc.qualified(stg)} ({cols}) using delta")
                    dbc.copy_into_from_s3(stg, copy_uri); dbc.merge_from_staging(table_name, stg, keys, list(integrated.columns)); dbc.execute(f"drop table if exists {dbc.qualified(stg)}")
                location = f"DATABRICKS::{dbc.qualified(table_name)}"; rows_written = len(integrated)
            elif use_merge and keys:
                stg = f"{table_name}__stg"; cols = ", ".join([f"`{c}` string" for c in integrated.columns]); dbc.execute(f"drop table if exists {dbc.qualified(stg)}"); dbc.execute(f"create table {dbc.qualified(stg)} ({cols}) using delta")
                dbc.write_df(integrated, stg, overwrite=False); dbc.execute(f"create table if not exists {dbc.qualified(table_name)} ({cols}) using delta"); dbc.merge_from_staging(table_name, stg, keys, list(integrated.columns)); dbc.execute(f"drop table if exists {dbc.qualified(stg)}")
                location = f"DATABRICKS::{dbc.qualified(table_name)}"; rows_written = len(integrated)
            else:
                rows_written = dbc.write_df(integrated, table=table_name, overwrite=False); location = f"DATABRICKS::{dbc.qualified(table_name)}"
        state["integration_df_csv"] = integrated.to_csv(index=False)
        state["integration_info"] = {"rows_written": rows_written, "table": table_name, "join_keys": keys, "scd_type": scd_type, "location": location}
        save_state(state)
        st.success(f"Integration complete. Rows written: {rows_written}")

# ----------------------------- Step 5 -----------------------------
elif step.startswith("5"):
    st.subheader("5) DWH Layer")
    if "integration_df_csv" not in state:
        st.warning("Run Integration first."); st.stop()
    integ_df = pd.read_csv(io.StringIO(state["integration_df_csv"]))
    st.write("Integration sample:"); st.dataframe(integ_df.head(20))

    use_sttm_dwh = st.checkbox("Use Excel STTM (sttm/STTM_template.xlsx) for DWH build", value=True, key="dwh_use_sttm")
    if use_sttm_dwh:
        try:
            sttm_df = load_sttm_excel(os.path.join("sttm","STTM_template.xlsx"))
            dwh_targets = build_dwh_targets(sttm_df)
            dwh_table = st.selectbox("Pick DWH target (from STTM)", dwh_targets, index=0, key="dwh_table_choice")
            scd_type = get_scd_for_target(sttm_df, "dwh", dwh_table, default_scd="SCD1")
            keys = get_keys_for_target(sttm_df, "dwh", dwh_table)
            # Editable defaults from STTM
            scd_type = st.selectbox("SCD type (from STTM, editable)", ["SCD1","SCD2","SCD3"], index=["SCD1","SCD2","SCD3"].index(scd_type if scd_type in ["SCD1","SCD2","SCD3"] else "SCD1"), key="dwh_scd_select")
            keys_text = st.text_input("Business key columns (from STTM, editable)", ",".join(keys), key="dwh_keys_input")
            keys = [c.strip() for c in keys_text.split(",") if c.strip()]
            st.info(f"STTM DWH plan → target={dwh_table}, scd={scd_type}, keys={keys}")
        except Exception as e:
            st.error(f"Failed to use STTM for DWH: {e}"); use_sttm_dwh=False
    if not use_sttm_dwh:
        st.warning("Manual DWH path not implemented in this build; enable STTM."); st.stop()

    use_merge = st.checkbox("Use native MERGE (where supported)", value=True, key="dwh_merge")
    use_s3_copy = st.checkbox("Prefer S3 COPY for bulk load (Snowflake/Redshift/Databricks)", value=False, key="dwh_copy")
    copy_prefix = st.text_input("S3 COPY prefix (DWH)", value=state.get("copy_prefix_dwh", f"s3://{state.get('s3_bucket','')}/{state.get('s3_prefix','agentic-etl')}/dwh"), key="dwh_copy_prefix")
    state["copy_prefix_dwh"] = copy_prefix

    if st.button("Run DWH Load", key="dwh_run"):
        target = state.get("dwh_target","Local CSV"); table_name = dwh_table
        rows_written = 0; location = ""
        if target == "Local CSV":
            existing = None
            path_exist = os.path.join("data","dwh",f"{table_name}.csv")
            if os.path.exists(path_exist): existing = pd.read_csv(path_exist)
            merged = TransformAgent().scd_load(existing, integ_df, scd_type, keys or [])
            location = write_local_df(merged, "dwh", table_name); rows_written = len(merged)
        elif target == "S3":
            s3 = S3Connector(region=state.get("s3_region","us-east-1")); uri = f"s3://{state.get('s3_bucket')}/{state.get('s3_prefix','agentic-etl')}/dwh/{table_name}.csv"
            s3.write_csv(integ_df, uri); location = uri; rows_written = len(integ_df)
        elif target == "Snowflake":
            sf = SnowflakeConnector(state.get("sf_user",""), state.get("sf_password",""), state.get("sf_account",""), state.get("sf_warehouse",""), state.get("sf_database",""), state.get("sf_schema",""), state.get("sf_role") or None)
            if use_s3_copy:
                from connectors.s3_connector import S3Connector as _S3; s3 = _S3(region=state.get("s3_region","us-east-1"))
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; s3.write_csv(integ_df, copy_uri)
                cols = ", ".join([f'"{c}" VARCHAR' for c in integ_df.columns]); sf.execute(f'create table if not exists {table_name} ({cols})')
                sf.copy_from_s3(table_name, copy_uri, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"))
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; sf.execute(f'create or replace table {stg} as select * from {table_name} where 1=2')
                    sf.copy_from_s3(stg, copy_uri, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"))
                    from utils.sql_merge import build_merge_sql; sf.execute(build_merge_sql("snowflake", table_name, stg, keys, list(integ_df.columns))); sf.execute(f'drop table if exists {stg}')
                location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"; rows_written = len(integ_df)
            elif use_merge and keys:
                rows_written = sf.merge_df(integ_df, target_table=table_name, keys=keys); location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"
            else:
                rows_written = sf.write_df(integ_df, table=table_name, overwrite=True); location = f"SNOWFLAKE::{state.get('sf_database','')}.{state.get('sf_schema','')}.{table_name}"
        elif target == "Redshift":
            rs = RedshiftConnector(state.get("rs_host",""), int(state.get("rs_port","5439")), state.get("rs_db",""), state.get("rs_user",""), state.get("rs_password",""), ssl=True)
            if use_s3_copy:
                from connectors.s3_connector import S3Connector as _S3; s3 = _S3(region=state.get("s3_region","us-east-1"))
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; s3.write_csv(integ_df, copy_uri)
                cols = ", ".join([f'"{c}" varchar' for c in integ_df.columns]); rs.execute(f'create table if not exists {table_name} ({cols});')
                rs.copy_from_s3(table_name, copy_uri, iam_role=None, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"), region=state.get("s3_region"))
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; rs.execute(f'drop table if exists {stg}; create table {stg} ({cols});')
                    rs.copy_from_s3(stg, copy_uri, iam_role=None, aws_key_id=state.get("aws_key"), aws_secret_key=state.get("aws_secret"), region=state.get("s3_region"))
                    from utils.sql_merge import build_merge_sql; rs.execute(build_merge_sql("redshift", table_name, stg, keys, list(integ_df.columns))); rs.execute(f'drop table if exists {stg}')
                location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"; rows_written = len(integ_df)
            elif use_merge and keys:
                stg = f"{table_name}__stg"; cols = ", ".join([f'"{c}" varchar' for c in integ_df.columns]); rs.execute(f'drop table if exists {stg}; create table {stg} ({cols});')
                rs.write_df(integ_df, table=stg, overwrite=False); from utils.sql_merge import build_merge_sql
                rs.execute(f'create table if not exists {table_name} ({cols});'); rs.execute(build_merge_sql("redshift", table_name, stg, keys, list(integ_df.columns))); rs.execute(f'drop table if exists {stg}')
                location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"; rows_written = len(integ_df)
            else:
                rows_written = rs.write_df(integ_df, table=table_name, overwrite=True); location = f"REDSHIFT::{state.get('rs_db','')}.public.{table_name}"
        elif target == "Databricks":
            dbc = DatabricksConnector(state.get("db_host",""), state.get("db_http",""), state.get("db_token",""), state.get("db_catalog") or None, state.get("db_schema") or None)
            if use_s3_copy:
                cols = ", ".join([f"`{c}` string" for c in integ_df.columns]); dbc.execute(f"create table if not exists {dbc.qualified(table_name)} ({cols}) using delta")
                copy_uri = f"{copy_prefix.rstrip('/')}/{table_name}_{state['run_id']}.csv"; from connectors.s3_connector import S3Connector as _S3; _S3(state.get("s3_region","us-east-1")).write_csv(integ_df, copy_uri)
                dbc.copy_into_from_s3(table_name, copy_uri)
                if use_merge and keys:
                    stg = f"{table_name}__copy_stg"; dbc.execute(f"drop table if exists {dbc.qualified(stg)}"); dbc.execute(f"create table {dbc.qualified(stg)} ({cols}) using delta")
                    dbc.copy_into_from_s3(stg, copy_uri); dbc.merge_from_staging(table_name, stg, keys, list(integ_df.columns)); dbc.execute(f"drop table if exists {dbc.qualified(stg)}")
                location = f"DATABRICKS::{dbc.qualified(table_name)}"; rows_written = len(integ_df)
            elif use_merge and keys:
                stg = f"{table_name}__stg"; cols = ", ".join([f"`{c}` string" for c in integ_df.columns]); dbc.execute(f"drop table if exists {dbc.qualified(stg)}"); dbc.execute(f"create table {dbc.qualified(stg)} ({cols}) using delta")
                dbc.write_df(integ_df, stg, overwrite=False); dbc.execute(f"create table if not exists {dbc.qualified(table_name)} ({cols}) using delta"); dbc.merge_from_staging(table_name, stg, keys, list(integ_df.columns)); dbc.execute(f"drop table if exists {dbc.qualified(stg)}")
                location = f"DATABRICKS::{dbc.qualified(table_name)}"; rows_written = len(integ_df)
            else:
                rows_written = dbc.write_df(integ_df, table=table_name, overwrite=True); location = f"DATABRICKS::{dbc.qualified(table_name)}"
        state["dwh_info"] = {"rows_written": rows_written, "table": table_name, "scd_type": scd_type, "location": location}
        save_state(state)
        st.success(f"DWH load complete. Rows written: {rows_written}")

# ----------------------------- Step 6 -----------------------------
elif step.startswith("6"):
    st.subheader("6) Reporting")
    if not all(k in state for k in ["landing_info","dq_report","integration_info","dwh_info"]):
        st.warning("Run previous steps first."); st.stop()
    context = {
        "run_id": state["run_id"],
        "when": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "config": {
            "source_type": state.get("source_type"),
            "landing_target": state.get("landing_target"),
            "integration_target": state.get("integration_target"),
            "dwh_target": state.get("dwh_target"),
        },
        "landing": state.get("landing_info",{}),
        "dq": state.get("dq_report",{}),
        "integration": state.get("integration_info",{}),
        "dwh": state.get("dwh_info",{}),
    }
    html = render_report_html("templates", context)
    out_dir = os.path.join("data","reports", state["run_id"])
    paths = save_html_and_pdf(html, out_dir, "run_report")
    st.success(f"Report generated: {paths['html']} {'& ' + paths['pdf'] if paths['pdf'] else ''}")
    st.markdown(f"[Open HTML]({paths['html']})", unsafe_allow_html=True)
    if paths['pdf']:
        st.markdown(f"[Open PDF]({paths['pdf']})", unsafe_allow_html=True)
    if st.checkbox("Email the report now", key="rep_email_now"):
        sender = state.get("email_sender"); recipients = [x.strip() for x in state.get("email_to","").split(",") if x.strip()]
        subject = f"Agentic ETL Report {state['run_id']}"
        if state.get("email_method","SMTP") == "SMTP":
            send_email_smtp(state.get("smtp_host","smtp.gmail.com"), int(state.get("smtp_port",587)), state.get("smtp_user"), state.get("smtp_pass"), sender, recipients, subject, html, [p for p in paths.values() if p])
        else:
            send_email_ses(state.get("ses_region","us-east-1"), sender, recipients, subject, html, [p for p in paths.values() if p])
        st.success("Email dispatched.")

# ----------------------------- Step 7 -----------------------------
else:
    st.subheader("7) Batch Runner (from STTM)")
    try:
        sttm_df = load_sttm_excel(os.path.join("sttm","STTM_template.xlsx"))
        integ_targets = sorted(sttm_df[sttm_df["Target Schema"].str.lower()=="integration"]["Target Table"].unique().tolist())
        dwh_targets = sorted(sttm_df[sttm_df["Target Schema"].str.lower()=="dwh"]["Target Table"].unique().tolist())
        st.write(f"Integration targets: {integ_targets}")
        st.write(f"DWH targets: {dwh_targets}")
        if st.button("Run all (local CSV only)", key="batch_run_all"):
            import pandas as _pd
            customers_df = _pd.read_csv("data/samples/customers.csv"); orders_df = _pd.read_csv("data/samples/orders.csv")
            write_local_df(customers_df, "landing", "customers"); write_local_df(orders_df, "landing", "orders")
            plan = build_integration_plan(sttm_df, "customers_int")
            dim_country = _pd.read_csv("data/samples/dim_country.csv")
            left = customers_df.merge(dim_country, left_on=plan["left_on"][0], right_on=plan["refs"][0]["df_key"], how="left")
            df_map = {("landing","customers"): left, ("reference","dim_country"): dim_country}
            integ_c = project_columns(df_map, plan["projection"]); write_local_df(integ_c, "integration", "customers_int")
            plan2 = build_integration_plan(sttm_df, "orders_int")
            dim_product = _pd.read_csv("data/samples/dim_product.csv")
            left2 = orders_df.merge(dim_product, left_on=plan2["left_on"][0], right_on=plan2["refs"][0]["df_key"], how="left")
            df_map2 = {("landing","orders"): left2, ("reference","dim_product"): dim_product}
            integ_o = project_columns(df_map2, plan2["projection"]); write_local_df(integ_o, "integration", "orders_int")
            dim_customer = integ_c[["customer_id","first_name","last_name","email","country_name"]]
            write_local_df(dim_customer, "dwh", "dim_customer")
            fact_orders = integ_o[["order_id","customer_id","product_id","qty","amount","order_date","product_name","category"]]
            write_local_df(fact_orders, "dwh", "fact_orders")
            st.success("Batch complete. Check data/integration & data/dwh.")
    except Exception as e:
        st.error(str(e))
