import os, json
import streamlit as st
import pandas as pd
from utils.config import load_config
from agents.sttm_reader import read_sttm_excel, REQUIRED_COLUMNS
from etl.io_local import read_source_csv, read_source_sqlite, write_target_csv, read_target_sqlite, write_target_sqlite, read_target_csv
from etl.io_cloud import test_connection, read_sql_table, write_sql_table
from etl.transformer import apply_rules
from etl.scd_handler import scd_type_1, scd_type_2, scd_type_3, deduplicate_source
from etl.llm_agent import extract_rules_from_sttm, generate_validations_from_sttm, validate_dataframe_summary

st.set_page_config(page_title="Agentic AI ETL — STTM (v4.5 full)", layout="wide")
cfg = load_config()

# Sidebar Connection Manager
if "connections" not in st.session_state:
    st.session_state.connections = {
        "local_csv": {"kind":"CSV","params":{"sample_path":"data/sample_initial.csv"}},
        "sqlite_local": {"kind":"SQLite","params":{"db_path":"data/target.db"}},
    }
if "selected_conn" not in st.session_state:
    st.session_state.selected_conn = next(iter(st.session_state.connections.keys()))

with st.sidebar:
    st.header("🔧 Connections")
    st.caption("Configure once, reuse everywhere. Test before running ETL.")
    if "pending_select_conn" in st.session_state:
        st.session_state.selected_conn = st.session_state.pending_select_conn
        del st.session_state["pending_select_conn"]
    conn_names = list(st.session_state.connections.keys()) or ["<none>"]
    def_idx = conn_names.index(st.session_state.selected_conn) if st.session_state.selected_conn in conn_names else 0
    selected = st.selectbox("Select connection", options=conn_names, index=def_idx)
    st.session_state.selected_conn = selected
    new_name_input = st.text_input("Create / rename connection", value=selected)
    new_name = new_name_input.strip()
    curr = st.session_state.connections.get(selected, {"kind":"CSV","params":{}})
    kind_options = ["CSV","SQLite","Databricks","Snowflake","Redshift"]
    kind = st.selectbox("Type", kind_options, index=kind_options.index(curr.get("kind","CSV")))
    params_in = curr.get("params", {})
    if kind == "CSV":
        p = {"sample_path": st.text_input("Sample CSV path (for preview)", params_in.get("sample_path","data/sample_initial.csv"))}
    elif kind == "SQLite":
        p = {"db_path": st.text_input("SQLite DB path", params_in.get("db_path","data/target.db"))}
    elif kind == "Databricks":
        p = {"server_hostname": st.text_input("Server Hostname", params_in.get("server_hostname","")),
             "http_path": st.text_input("HTTP Path", params_in.get("http_path","")),
             "access_token": st.text_input("Access Token", params_in.get("access_token",""), type="password"),
             "catalog": st.text_input("Catalog (optional)", params_in.get("catalog","")),
             "schema": st.text_input("Schema (optional)", params_in.get("schema",""))}
    elif kind == "Snowflake":
        p = {"account": st.text_input("Account", params_in.get("account","")),
             "user": st.text_input("User", params_in.get("user","")),
             "password": st.text_input("Password", params_in.get("password",""), type="password"),
             "warehouse": st.text_input("Warehouse", params_in.get("warehouse","")),
             "database": st.text_input("Database", params_in.get("database","")),
             "schema": st.text_input("Schema", params_in.get("schema","")),
             "role": st.text_input("Role (optional)", params_in.get("role",""))}
    else:
        p = {"host": st.text_input("Host", params_in.get("host","")),
             "port": st.number_input("Port", 1, 65535, int(params_in.get("port",5439))),
             "database": st.text_input("Database", params_in.get("database","")),
             "user": st.text_input("User", params_in.get("user","")),
             "password": st.text_input("Password", params_in.get("password",""), type="password")}
    col_s, col_t = st.columns(2)
    def _save_connection():
        if not new_name or new_name == "<none>":
            st.error("Please enter a valid connection name."); return
        if new_name != selected and selected in st.session_state.connections:
            st.session_state.connections.pop(selected, None)
        st.session_state.connections[new_name] = {"kind": kind, "params": p}
        st.session_state.pending_select_conn = new_name
        st.success(f"Saved connection '{new_name}'."); st.rerun()
    with col_s:
        if st.button("💾 Save Connection"):
            _save_connection()
    with col_t:
        if kind in ["Databricks","Snowflake","Redshift"] and st.button("🔌 Test Connection"):
            try:
                msg = test_connection(kind, p)
                st.success(f"{kind} connection OK: {msg}")
            except Exception as e:
                st.error(f"{kind} connection failed: {e}")
    st.divider(); st.caption("Tip: DEFAULT_SOURCE_CONN / DEFAULT_TARGET_CONN in .env")

def pick_connection(name_key: str, label: str):
    conns = st.session_state.connections
    conn_names = list(conns.keys())
    if name_key not in st.session_state or st.session_state[name_key] not in conn_names:
        pref = st.session_state.get("selected_conn")
        st.session_state[name_key] = pref if pref in conn_names else (conn_names[0] if conn_names else "")
    return st.selectbox(label, options=conn_names, index=conn_names.index(st.session_state[name_key]), key=name_key)

if "sttm_df" not in st.session_state: st.session_state.sttm_df=None
if "rules_df" not in st.session_state: st.session_state.rules_df=None
if "src_df" not in st.session_state: st.session_state.src_df=None
if "validations" not in st.session_state: st.session_state.validations=[]

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Source & Target", "SCD & Processing", "Transformation Rules", "LLM Assist & Validation", "Run ETL"])

with tab1:
    st.header("Source & Target")
    conns = st.session_state.connections
    source_conn_name = pick_connection("source_conn_name", "Source connection")
    src_kind = conns[source_conn_name]["kind"]
    st.write(f"Source type: **{src_kind}**")
    if src_kind=="CSV":
        mode = st.radio("CSV mode", ["Upload file","Use saved path"], horizontal=True)
        if mode=="Upload file":
            up_src = st.file_uploader("Upload Source CSV", type=["csv"])
            if up_src is not None:
                try:
                    df = pd.read_csv(up_src)
                    st.session_state.src_df = df
                    st.success("Loaded source from upload.")
                    st.dataframe(df.head(50), use_container_width=True)
                except Exception as e:
                    st.error(f"Failed to read uploaded CSV: {e}")
        else:
            src_csv_path = st.text_input("Path", conns[source_conn_name]["params"].get("sample_path","data/sample_initial.csv"))
            if st.button("Preview CSV Source"):
                try:
                    st.session_state.src_df = read_source_csv(src_csv_path)
                    st.dataframe(st.session_state.src_df.head(50))
                except Exception as e:
                    st.error(f"Read CSV failed: {e}")
    elif src_kind=="SQLite":
        db_path = st.text_input("SQLite path", conns[source_conn_name]["params"].get("db_path","data/target.db"))
        table = st.text_input("Source table", "source_customers")
        if st.button("Preview SQLite Source"):
            try:
                st.session_state.src_df = read_source_sqlite(db_path, table)
                st.dataframe(st.session_state.src_df.head(50))
            except Exception as e:
                st.error(f"Read SQLite failed: {e}")
    else:
        table = st.text_input("Source table (leave empty to run custom query)", "")
        query = st.text_area("Custom SQL query (optional)", "", height=120)
        if st.button(f"Preview {src_kind} Source"):
            try:
                df = read_sql_table(src_kind, conns[source_conn_name]["params"], table=table, query=query)
                st.session_state.src_df = df
                st.success(f"Loaded {len(df)} rows from {src_kind}.")
                st.dataframe(df.head(50), use_container_width=True)
            except Exception as e:
                st.error(f"Read {src_kind} failed: {e}")
    st.subheader("Upload STTM (Excel)")
    up = st.file_uploader("Upload STTM Excel (.xlsx)", type=["xlsx"])
    if up:
        try:
            st.session_state.sttm_df = read_sttm_excel(up)
            st.success("Loaded STTM from upload.")
            st.dataframe(st.session_state.sttm_df, use_container_width=True)
        except Exception as e:
            st.error(f"STTM validation failed: {e}")
    st.caption("Or use the sample bundled: docs/STTM_sample.xlsx")
    st.divider()
    target_conn_name = pick_connection("target_conn_name", "Target connection")
    tgt_kind = conns[target_conn_name]["kind"]
    st.write(f"Target type: **{tgt_kind}**")
    if tgt_kind=="CSV":
        st.session_state.target_csv = st.text_input("Target CSV path", "output/dim_customer.csv")
    elif tgt_kind=="SQLite":
        st.session_state.target_db = st.text_input("SQLite path", conns[target_conn_name]["params"].get("db_path","data/target.db"))
        st.session_state.target_table = st.text_input("Target table", "dim_customer")
    else:
        st.session_state.target_table_cloud = st.text_input(f"Target table ({tgt_kind})", "dim_customer")
        st.session_state.target_write_mode = st.selectbox("Write mode", ["replace","append"], index=0)

with tab2:
    st.header("SCD & Processing")
    load_mode_label = st.selectbox("Batch mode", ["Snapshot (full)","Incremental (delta)"], index=1)
    is_snapshot = load_mode_label.startswith("Snapshot")
    st.subheader("Duplicate Handling (SCD2/3)")
    dedup_strategy = st.selectbox("On duplicate keys", ["fail","keep_first","keep_last","by_timestamp"], index=2)
    dedup_ts_col = st.text_input("Timestamp column (for 'by_timestamp')", "last_updated")
    write_dup_audit = st.checkbox("Write duplicate audit CSV", True)
    st.divider()
    scd_type = st.radio("SCD Type", ["SCD1","SCD2","SCD3"], index=1)
    business_key = st.text_input("Business key (target col)", "customer_key")
    auto_tracked = st.checkbox("Auto-infer tracked cols", True)
    tracked_cols_text = st.text_input("Tracked columns (if auto-infer OFF)", "email,city,country_code,segment,total_with_tax,email_domain,full_name")
    eff_start = st.text_input("effective_start", "effective_start")
    eff_end = st.text_input("effective_end", "effective_end")
    current_flag = st.text_input("is_current", "is_current")
    with st.expander("Advanced SCD2"):
        version_col = st.text_input("version", "version")
        surrogate_key_col = st.text_input("surrogate key (optional)", "")
        soft_delete = st.checkbox("Soft delete missing (Snapshot only)", True)
    preview_before_load = st.checkbox("Preview before load", True)
    enable_rule_preview = st.checkbox("Enable rule preview", True)
    dry_run = st.checkbox("Dry run (no write)", False)
    st.session_state.run_opts = dict(
        load_mode="Snapshot" if is_snapshot else "Incremental",
        dedup_strategy=dedup_strategy, dedup_ts_col=dedup_ts_col, write_dup_audit=write_dup_audit,
        scd_type=scd_type, business_key=business_key, auto_tracked=auto_tracked, tracked_cols_text=tracked_cols_text,
        eff_start=eff_start, eff_end=eff_end, current_flag=current_flag,
        version_col=version_col, surrogate_key_col=surrogate_key_col or None, soft_delete=soft_delete,
        preview=preview_before_load, rule_preview=enable_rule_preview, dry_run=dry_run,
        target_conn=target_conn_name, target_kind=tgt_kind, source_conn=source_conn_name, source_kind=src_kind
    )

with tab3:
    st.header("Transformation Rules")
    if st.session_state.sttm_df is not None:
        st.info("DSL: trim(x), lower(x), upper(x), title(x), split(email,'@')[1], concat(first_name, ' ', last_name), mul(col,1.1), add(col,5), filter_year(col,YYYY).")
        st.session_state.rules_df = st.data_editor(st.session_state.sttm_df, num_rows="dynamic", use_container_width=True, key="rules_editor")
    else:
        st.warning("Upload STTM in Tab 1.")

with tab4:
    st.header("LLM Assist & Validation (Optional)")
    st.write("LLM is used only when you click a button below.")
    model = st.text_input("Model", os.getenv("OPENAI_MODEL","gpt-4o-mini"))
    temp = st.number_input("Temperature", 0.0, 1.0, float(cfg.get('llm',{}).get('temperature',0.0)), 0.1)
    max_tokens = st.number_input("Max tokens", 256, 8000, int(cfg.get('llm',{}).get('max_tokens',2000)), 256)
    sample_rows = st.number_input("Rows to sample for validation (0=schema only)", 0, 500, int(os.getenv("LLM_SAMPLE_ROWS", cfg.get('llm',{}).get('sample_rows',50))), 10)
    c1,c2,c3 = st.columns(3)
    with c1:
        if st.button("Parse STTM → JSON rules (LLM)"):
            if st.session_state.sttm_df is None:
                st.error("Upload STTM first.")
            else:
                rows = st.session_state.sttm_df.to_dict(orient="records")
                try:
                    data = extract_rules_from_sttm(rows, model=model, temperature=float(temp), max_tokens=int(max_tokens))
                    st.success("Parsed. See JSON below.")
                    st.json(data, expanded=False)
                    if data.get("tracked_cols"):
                        st.session_state.run_opts["auto_tracked"]=False
                        st.session_state.run_opts["tracked_cols_text"]=",".join(data["tracked_cols"])
                        st.info(f"Tracked columns set: {st.session_state.run_opts['tracked_cols_text']}")
                    if data.get("validations"):
                        st.session_state.validations=data["validations"]
                        st.info(f"Loaded {len(st.session_state.validations)} validations from LLM.")
                except Exception as e:
                    st.error(f"LLM parse failed: {e}")
    with c2:
        if st.button("Suggest Validation Rules (LLM)"):
            if st.session_state.sttm_df is None:
                st.error("Upload STTM first.")
            else:
                try:
                    v = generate_validations_from_sttm(st.session_state.sttm_df.to_dict(orient="records"), model=model, temperature=float(temp), max_tokens=int(max_tokens))
                    st.session_state.validations = v.get("validations", [])
                    st.success(f"Suggested {len(st.session_state.validations)} rules.")
                    st.json(v, expanded=False)
                except Exception as e:
                    st.error(f"Suggestion failed: {e}")
    with c3:
        if st.button("Run PRE-LOAD Validation (LLM)"):
            if st.session_state.src_df is None:
                st.error("Load source first.")
            else:
                try:
                    summary = validate_dataframe_summary(st.session_state.src_df, {"validations":st.session_state.validations}, "pre-load", model, float(temp), int(max_tokens), int(sample_rows))
                    st.success("Pre-load validation summary")
                    st.json(summary, expanded=False)
                except Exception as e:
                    st.error(f"Pre-load validation failed: {e}")

with tab5:
    st.header("Run ETL")
    if st.session_state.src_df is None:
        st.warning("Load source in Tab 1 first.")
    if st.session_state.rules_df is None:
        st.warning("Prepare rules in Tab 3.")
    if st.session_state.src_df is not None and st.session_state.rules_df is not None:
        st.subheader("Summary")
        st.write("Source rows:", len(st.session_state.src_df))
        st.write("Rules:", len(st.session_state.rules_df))
        opts = st.session_state.run_opts
        out_preview = apply_rules(st.session_state.src_df, st.session_state.rules_df)
        run = st.button("Execute ETL Now", type="primary")
        if run:
            src_df = st.session_state.src_df.copy()
            out_df = apply_rules(src_df, st.session_state.rules_df)
            scd_type = opts.get("scd_type")
            load_mode = opts.get("load_mode")
            if scd_type == "SCD1":
                final_df = scd_type_1(out_df, None, audit_cols={"batch_id":"local_demo","loaded_at":pd.Timestamp.utcnow()})
            else:
                bk = opts.get("business_key")
                if bk and opts.get("write_dup_audit", True) and bk in out_df.columns:
                    dmask = out_df.duplicated(subset=[bk], keep=False)
                    if int(dmask.sum())>0:
                        os.makedirs("output", exist_ok=True)
                        ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
                        out_df[dmask].to_csv(f"output/duplicate_audit_{bk}_{ts}.csv", index=False)
                        st.info("Duplicate audit CSV written in ./output")
                try:
                    out_df = deduplicate_source(out_df, bk, opts.get("dedup_strategy","keep_last"), opts.get("dedup_ts_col"))
                except Exception as e:
                    st.error(f"Dedup failed: {e}"); st.stop()
                existing=None
                if opts.get("target_kind")=="CSV":
                    existing = read_target_csv(st.session_state.get("target_csv","output/dim_customer.csv"))
                elif opts.get("target_kind")=="SQLite":
                    existing = read_target_sqlite(st.session_state.get("target_db","data/target.db"), st.session_state.get("target_table","dim_customer"))
                if opts.get("auto_tracked", True) and st.session_state.rules_df is not None:
                    mapped = [str(x).strip() for x in st.session_state.rules_df["Target Column"].tolist() if str(x).strip()]
                    tech = {bk, 'effective_start','effective_end','is_current','version','batch_id','loaded_at'}
                    tracked = [c for c in mapped if c not in tech]
                else:
                    tracked = [c.strip() for c in opts.get("tracked_cols_text","").split(",") if c.strip()]
                if scd_type == "SCD2":
                    final_df = scd_type_2(out_df, existing, business_key=bk, tracked_cols=tracked,
                                          eff_start=opts["eff_start"], eff_end=opts["eff_end"], current_flag=opts["current_flag"],
                                          version_col=opts["version_col"], surrogate_key_col=opts.get("surrogate_key_col"),
                                          soft_delete=opts.get("soft_delete", True),
                                          load_mode=load_mode,
                                          audit_cols={"batch_id":"local_demo","loaded_at":pd.Timestamp.utcnow()})
                else:
                    final_df = scd_type_3(out_df, existing, keys=[bk], tracked_cols=tracked,
                                          audit_cols={"batch_id":"local_demo","loaded_at":pd.Timestamp.utcnow()})
            st.subheader("Transformed Preview")
            st.dataframe(final_df.head(300))
            if st.checkbox("Run POST-LOAD Validation (LLM)", value=False):
                try:
                    summary = validate_dataframe_summary(final_df, {"validations":st.session_state.validations}, "post-load", os.getenv("OPENAI_MODEL","gpt-4o-mini"), float(cfg.get('llm',{}).get('temperature',0.0)), int(cfg.get('llm',{}).get('max_tokens',2000)), int(os.getenv("LLM_SAMPLE_ROWS", cfg.get('llm',{}).get('sample_rows',50))))
                    st.success("Post-load validation summary")
                    st.json(summary, expanded=False)
                except Exception as e:
                    st.error(f"Post-load validation failed: {e}")
            if not opts.get("dry_run"):
                tgt_kind = st.session_state.run_opts.get("target_kind")
                if tgt_kind=="CSV":
                    write_target_csv(final_df, st.session_state.get("target_csv","output/dim_customer.csv"))
                    st.success("Wrote CSV.")
                elif tgt_kind=="SQLite":
                    write_target_sqlite(final_df, st.session_state.get("target_db","data/target.db"), st.session_state.get("target_table","dim_customer"))
                    st.success("Wrote SQLite.")
                else:
                    tname = st.session_state.get("target_table_cloud","dim_customer")
                    mode = st.session_state.get("target_write_mode","replace")
                    try:
                        write_sql_table(tgt_kind, st.session_state.connections[opts.get("target_conn")]["params"], table=tname, df=final_df, if_exists=mode)
                        st.success(f"Wrote to {tgt_kind}: {tname}")
                    except Exception as e:
                        st.error(f"Write to {tgt_kind} failed: {e}")
            else:
                st.info("Dry run ON — no write.")
