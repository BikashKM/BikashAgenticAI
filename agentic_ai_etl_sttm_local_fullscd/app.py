import os, json, tempfile, time
import streamlit as st
import pandas as pd
from utils.config import load_config
from agents.sttm_reader import read_sttm_excel, REQUIRED_COLUMNS
from etl.io_local import read_source_csv, read_source_sqlite, write_target_csv, read_target_sqlite, write_target_sqlite, read_target_csv
from etl.transformer import apply_rules
from etl.scd_handler import scd_type_1, scd_type_2, scd_type_3, deduplicate_source

st.set_page_config(page_title="Agentic AI ETL — STTM (Local, v4.1 - Incremental Safe)", layout="wide")
cfg = load_config()

if "sttm_df" not in st.session_state: st.session_state.sttm_df=None
if "rules_df" not in st.session_state: st.session_state.rules_df=None
if "src_df" not in st.session_state: st.session_state.src_df=None
if "src_path" not in st.session_state: st.session_state.src_path=None

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Source & Target", "SCD & Processing", "Transformation Rules", "LLM Config", "Run ETL"])

with tab1:
    st.header("Source & Target")
    src_type = st.radio("Source type", ["CSV","SQLite"], horizontal=True, index=0)
    if src_type=="CSV":
        input_mode = st.radio("Select source CSV input method", ["Upload file","Enter path"], horizontal=True)
        if input_mode=="Upload file":
            up_src = st.file_uploader("Upload Source CSV", type=["csv"])
            if up_src is not None:
                try:
                    df = pd.read_csv(up_src)
                    st.session_state.src_df = df
                    os.makedirs("data", exist_ok=True)
                    saved_path = f"data/uploaded_source_{int(time.time())}.csv"
                    df.to_csv(saved_path, index=False)
                    st.session_state.src_path = saved_path
                    st.success(f"Loaded source from upload. Saved a copy at: {saved_path}")
                    st.dataframe(df.head(50), use_container_width=True)
                except Exception as e:
                    st.error(f"Failed to read uploaded CSV: {e}")
        else:
            src_csv_path = st.text_input("Source CSV path", "data/sample_initial.csv")
            if st.button("Preview CSV Source"):
                try:
                    st.session_state.src_df = read_source_csv(src_csv_path)
                    st.session_state.src_path = src_csv_path
                    st.dataframe(st.session_state.src_df.head(50))
                except Exception as e:
                    st.error(f"Read CSV failed: {e}")
    else:
        src_db = st.text_input("SQLite path", "data/source.db")
        src_table = st.text_input("SQLite source table", "source_customers")
        if st.button("Preview SQLite Source"):
            try:
                st.session_state.src_df = read_source_sqlite(src_db, src_table)
                st.session_state.src_path = f"sqlite://{src_db}::{src_table}"
                st.dataframe(st.session_state.src_df.head(50))
            except Exception as e:
                st.error(f"Read SQLite failed: {e}")

    st.subheader("Upload STTM (Excel)")
    up = st.file_uploader("Upload STTM Excel (.xlsx)", type=["xlsx"])
    if up:
        try:
            tmp = os.path.join(tempfile.gettempdir(), f"sttm_{int(time.time())}.xlsx")
            with open(tmp, "wb") as f: f.write(up.getbuffer())
            st.session_state.sttm_df = read_sttm_excel(tmp)
            st.success(f"Loaded STTM from upload: {tmp}")
            st.dataframe(st.session_state.sttm_df, use_container_width=True)
        except Exception as e:
            st.error(f"STTM validation failed: {e}")
    st.caption("Or use the sample bundled: docs/STTM_sample.xlsx")

    st.divider()
    target_type = st.radio("Target type", ["CSV","SQLite"], horizontal=True, index=0)
    if target_type=="CSV":
        st.session_state.target_csv = st.text_input("Target CSV path", "output/dim_customer.csv")
    else:
        st.session_state.target_db = st.text_input("Target SQLite path", "data/target.db")
        st.session_state.target_table = st.text_input("Target table name", "dim_customer")

with tab2:
    st.header("SCD & Processing")
    st.subheader("Load Mode")
    load_mode_label = st.selectbox("How is this batch fed?", ["Snapshot (full)", "Incremental (delta)"], index=1)
    is_snapshot = load_mode_label.startswith("Snapshot")
    st.caption("Snapshot: full image of all keys; Incremental: only changed/new keys — no expirations are applied.")

    st.subheader("Duplicate Handling (applies to SCD2/SCD3 only)")
    dedup_strategy = st.selectbox("On duplicate business keys", ["fail","keep_first","keep_last","by_timestamp"], index=2)
    dedup_ts_col = st.text_input("Timestamp column (required for 'by_timestamp')", "last_updated")
    write_dup_audit = st.checkbox("Write Duplicate Audit CSV before dedup (SCD2/SCD3)", True)
    st.divider()

    scd_type = st.radio("SCD Type", ["SCD1","SCD2","SCD3"], index=1)
    business_key = st.text_input("Business key (Target Column name)", "customer_key")

    auto_tracked = st.checkbox("Auto-infer tracked columns (all mapped except business key & SCD/audit cols)", True)
    tracked_cols_text = st.text_input("Tracked columns (comma-separated; ignored if auto-infer is ON)", "email,city,country_code,segment,total_with_tax,email_domain,full_name")

    eff_start = st.text_input("effective_start", "effective_start")
    eff_end = st.text_input("effective_end", "effective_end")
    current_flag = st.text_input("is_current", "is_current")
    with st.expander("Advanced SCD2 Options", expanded=False):
        version_col = st.text_input("version column", "version")
        surrogate_key_col = st.text_input("surrogate key column (optional)", "")
        soft_delete = st.checkbox("Soft delete missing keys (Snapshot mode only)", True)
    preview_before_load = st.checkbox("Preview before load", True)
    enable_rule_preview = st.checkbox("Enable rule preview before execution", True)
    dry_run = st.checkbox("Dry run (no write)", False)

    st.session_state.run_opts = dict(
        load_mode="Snapshot" if is_snapshot else "Incremental",
        dedup_strategy=dedup_strategy, dedup_ts_col=dedup_ts_col, write_dup_audit=write_dup_audit,
        scd_type=scd_type, business_key=business_key, auto_tracked=auto_tracked, tracked_cols_text=tracked_cols_text,
        eff_start=eff_start, eff_end=eff_end, current_flag=current_flag,
        version_col=version_col, surrogate_key_col=surrogate_key_col or None, soft_delete=soft_delete,
        preview=preview_before_load, rule_preview=enable_rule_preview, dry_run=dry_run,
        target_type=target_type
    )

with tab3:
    st.header("Transformation Rules")
    if st.session_state.sttm_df is not None:
        st.info("Edit rules below. DSL: trim(x), lower(x), upper(x), title(x), split(email,'@')[1], concat(first_name, ' ', last_name), mul(col,1.1), add(col,5), filter_year(col,YYYY).")
        edited = st.data_editor(st.session_state.sttm_df, num_rows="dynamic", use_container_width=True, key="rules_editor")
        st.session_state.rules_df = edited
    else:
        st.warning("Upload or load STTM in Tab 1.")

with tab4:
    st.header("LLM Config (read-only view)")
    st.code(open(".env.example","r").read(), language="ini")
    st.code(open("config.yaml","r").read(), language="yaml")

with tab5:
    st.header("Run ETL")
    if st.session_state.src_df is None:
        st.warning("Load source in Tab 1 first (upload or path).")
    if st.session_state.rules_df is None:
        st.warning("Prepare rules in Tab 3 (upload STTM).")
    if st.session_state.src_df is not None and st.session_state.rules_df is not None:
        st.subheader("Summary")
        st.write("Source rows:", len(st.session_state.src_df))
        st.write("Rules:", len(st.session_state.rules_df))

        opts = st.session_state.run_opts
        out_preview = apply_rules(st.session_state.src_df, st.session_state.rules_df)

        # Expiration preview
        if opts.get("scd_type") == "SCD2":
            if opts["target_type"] == "SQLite":
                existing = read_target_sqlite(st.session_state.get("target_db","data/target.db"), st.session_state.get("target_table","dim_customer"))
            else:
                existing = read_target_csv(st.session_state.get("target_csv","output/dim_customer.csv"))
            if existing is not None and len(existing)>0 and opts.get("business_key") in out_preview.columns and "is_current" in existing.columns:
                src_keys = set(pd.Series(out_preview[opts["business_key"]]).dropna().tolist())
                cur = existing[existing["is_current"]==True]
                would_expire = cur[~cur[opts["business_key"]].isin(src_keys)]
                if opts.get("load_mode")=="Snapshot" and opts.get("soft_delete", True):
                    st.warning(f"Snapshot mode: {len(would_expire)} current rows would be expired.")
                else:
                    st.info("Incremental mode: 0 rows will be expired (missing keys are ignored).")
                with st.expander("Show keys that would be expired (Snapshot only)"):
                    if len(would_expire)>0:
                        st.dataframe(would_expire[[opts["business_key"]]].head(200))
                    else:
                        st.caption("Nothing to expire.")

        if st.button("Execute ETL Now", type="primary"):
            src_df = st.session_state.src_df.copy()
            rules_df = st.session_state.rules_df.copy()
            opts = st.session_state.run_opts
            out_df = apply_rules(src_df, rules_df)

            scd_type = opts.get("scd_type")
            load_mode = opts.get("load_mode")

            if scd_type == "SCD1":
                final_df = scd_type_1(out_df, None, audit_cols={"batch_id":"local_demo","loaded_at":pd.Timestamp.utcnow()})
            else:
                bk = opts.get("business_key")

                # Optional duplicate audit
                if bk and opts.get("write_dup_audit", False) and bk in out_df.columns:
                    dmask = out_df.duplicated(subset=[bk], keep=False)
                    if int(dmask.sum())>0:
                        os.makedirs("output", exist_ok=True)
                        ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
                        audit_path = f"output/duplicate_audit_{bk}_{ts}.csv"
                        out_df[dmask].to_csv(audit_path, index=False)
                        st.info(f"Wrote duplicate audit CSV: {audit_path}")

                # Dedup
                try:
                    out_df = deduplicate_source(out_df, bk, opts.get("dedup_strategy","keep_last"), opts.get("dedup_ts_col"))
                except Exception as e:
                    st.error(f"Deduplication failed: {e}"); st.stop()

                # Existing
                if opts["target_type"] == "SQLite":
                    existing = read_target_sqlite(st.session_state.get("target_db","data/target.db"), st.session_state.get("target_table","dim_customer"))
                else:
                    existing = read_target_csv(st.session_state.get("target_csv","output/dim_customer.csv"))

                # Tracked columns
                if opts.get("auto_tracked", True) and rules_df is not None:
                    mapped = [str(x).strip() for x in rules_df["Target Column"].tolist() if str(x).strip()]
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

            if not opts.get("dry_run"):
                if opts["target_type"] == "CSV":
                    write_target_csv(final_df, st.session_state.get("target_csv","output/dim_customer.csv"))
                    st.success(f"Wrote {len(final_df)} rows to CSV: {st.session_state.get('target_csv','output/dim_customer.csv')}")
                else:
                    write_target_sqlite(final_df, st.session_state.get("target_db","data/target.db"), st.session_state.get("target_table","dim_customer"))
                    st.success(f"Wrote {len(final_df)} rows to SQLite: {st.session_state.get('target_db','data/target.db')}::{st.session_state.get('target_table','dim_customer')}")
            else:
                st.info("Dry run enabled — no data written.")
