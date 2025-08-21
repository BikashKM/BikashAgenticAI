from __future__ import annotations
import streamlit as st, pandas as pd, os, io, uuid, datetime as dt, json
from utils.session_store import load_state, save_state
from utils.sttm_parser import load_sttm_excel, validate_sttm, build_integration_plan, project_columns, get_scd_for_target, get_keys_for_target, build_dwh_targets
from agents.dq_agent import DQAgent
from agents.transform_agent import TransformAgent
from agents.report_agent import render_report_html, save_html_and_pdf
from connectors.local_csv import write_df as write_local_df
from utils.llm_gateway import infer_plan_from_prompt

st.set_page_config(page_title="Agentic ETL — Guided (Local CSV, LLM)", layout="wide")
st.title("🤖 Agentic ETL — Guided (Local CSV, LLM-enabled)")

state = load_state()
if "run_id" not in state: state["run_id"] = uuid.uuid4().hex[:8]
if "chat" not in state:
    state["chat"] = [{"role":"assistant","content":"Tell me what to onboard. I can parse your instruction with an LLM if OPENAI_API_KEY is set. Nothing runs until you click Generate Plan."}]
if "job" not in state:
    state["job"] = {"sources": [], "targets": {"landing":"Local CSV","integration":"Local CSV","dwh":"Local CSV"}, "dq": {}, "sttm": None, "plan": None, "hints": {}}
save_state(state)

def add_msg(role, content):
    state["chat"].append({"role": role, "content": content}); save_state(state)

for m in state["chat"]:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

prompt = st.chat_input("Describe your ETL task…", key="chat_input")
if prompt:
    add_msg("user", prompt)
    try:
        plan = infer_plan_from_prompt(prompt)
        if plan.get("steps"):
            state["job"]["plan"] = {"steps": plan["steps"], "integration_targets": []}
        if plan.get("scd_hint"): state["job"]["hints"]["scd"] = plan["scd_hint"]
        if plan.get("use_samples"):
            existing_names = set(s["name"] for s in state["job"]["sources"])
            if "customers" not in existing_names:
                with open(os.path.join("data","samples","customers.csv"), "rb") as f:
                    state["job"]["sources"].append({"id": uuid.uuid4().hex[:6], "name":"customers","type":"Local CSV","payload_bytes": f.read(),"size": os.path.getsize(os.path.join("data","samples","customers.csv"))})
            if "orders" not in existing_names:
                with open(os.path.join("data","samples","orders.csv"), "rb") as f:
                    state["job"]["sources"].append({"id": uuid.uuid4().hex[:6], "name":"orders","type":"Local CSV","payload_bytes": f.read(),"size": os.path.getsize(os.path.join("data","samples","orders.csv"))})
            add_msg("assistant","I added sample sources: customers, orders.")
        add_msg("assistant", f"Parsed intent:```json\n{json.dumps(plan, indent=2)}\n``` Upload STTM next, click **Generate Plan**, then **Run Selected Steps**.")
        save_state(state)
    except Exception as e:
        add_msg("assistant", f"LLM parsing unavailable ({e}). Please proceed to upload sources/STTM and Generate Plan.")

st.markdown("---")
st.subheader("🧩 Sources (Local CSV)")
col = st.columns([2,1,1,1])
with col[0]:
    up = st.file_uploader("Upload CSV (add multiple, one by one)", type="csv", key="upl_csv")
with col[1]:
    add_btn = st.button("Add source", key="btn_add_src")
with col[2]:
    clear_btn = st.button("Clear all sources", key="btn_clear_src")
with col[3]:
    reset_btn = st.button("Reset session", key="btn_reset")

if reset_btn:
    import shutil
    if os.path.exists(".run_state"): shutil.rmtree(".run_state")
    st.experimental_rerun()

if clear_btn:
    state["job"]["sources"] = []; save_state(state); st.success("All sources cleared.")

if add_btn:
    if up is None:
        st.error("Please choose a CSV first.")
    else:
        name = os.path.splitext(up.name)[0]
        state["job"]["sources"].append({"id": uuid.uuid4().hex[:6], "name": name, "type":"Local CSV", "payload_bytes": up.getvalue(), "size": len(up.getvalue())})
        save_state(state); st.success(f"Added source: {up.name}")

srcs = state["job"]["sources"]
if srcs:
    st.dataframe(pd.DataFrame([{"id":s["id"],"name":s["name"],"type":s["type"],"size(bytes)":s["size"]} for s in srcs]))
    rm_cols = st.columns(len(srcs))
    for i,s in enumerate(list(srcs)):
        with rm_cols[i]:
            if st.button(f"Remove {s['name']}", key=f"rm_{s['id']}"):
                state["job"]["sources"] = [x for x in state["job"]["sources"] if x["id"]!=s["id"]]; save_state(state); st.experimental_rerun()

st.markdown("---")
st.subheader("📑 STTM (Excel)")
c1, c2 = st.columns([2,1])
with c1:
    sttm_up = st.file_uploader("Upload STTM Excel", type=["xlsx"], key="sttm_upload")
    if sttm_up:
        b = sttm_up.read()
        try:
            sttm_df = load_sttm_excel(b); validate_sttm(sttm_df)
            state["job"]["sttm"] = b; save_state(state)
            st.success("STTM uploaded and parsed.")
        except Exception as e:
            st.error(f"Failed to parse STTM: {e}")
with c2:
    if st.button("Use built-in sample STTM", key="btn_use_sample_sttm"):
        with open(os.path.join("sttm","STTM_template.xlsx"), "rb") as f: state["job"]["sttm"] = f.read()
        save_state(state); st.success("Loaded sample STTM.")
if state["job"]["sttm"]:
    sttm_df = load_sttm_excel(state["job"]["sttm"]); validate_sttm(sttm_df)
    st.markdown("**Edit STTM inline (optional):**")
    edited = st.data_editor(sttm_df, key="sttm_editor", num_rows="dynamic")
    if st.button("Save edited STTM to session", key="btn_save_sttm_edit"):
        buf = io.BytesIO(); edited.to_excel(buf, index=False); state["job"]["sttm"] = buf.getvalue(); save_state(state); st.success("STTM saved.")

st.markdown("---")
st.subheader("🗺️ Plan")
plan_col1, plan_col2 = st.columns([1,1])
with plan_col1:
    steps_default = state["job"].get("plan",{}).get("steps", ["Landing","DQ","Integration","DWH","Report"])
    steps = st.multiselect("Steps to run", ["Landing","DQ","Integration","DWH","Report"], default=steps_default, key="run_steps")
with plan_col2:
    state["job"]["dq"]["approve_on_critical"] = st.checkbox("Auto-approve CRITICAL DQ failures", value=state["job"]["dq"].get("approve_on_critical", False), key="dq_auto")

if st.button("Generate Plan", key="btn_gen_plan"):
    missing = []
    if not state["job"]["sources"]: missing.append("at least one Source CSV")
    if not state["job"]["sttm"]: missing.append("an STTM Excel")
    if missing:
        st.error("Please provide " + " and ".join(missing) + " before generating a plan.")
    else:
        sttm_df = load_sttm_excel(state["job"]["sttm"]); validate_sttm(sttm_df)
        tables = sorted(sttm_df[(sttm_df["Target Schema"].str.lower()=="integration")]["Target Table"].unique().tolist())
        state["job"]["plan"] = {"steps": steps, "integration_targets": tables}
        save_state(state)
        st.success("Plan generated. Review below and click **Run Selected Steps**.")

if state["job"].get("plan"):
    st.write("**Planned steps:**", state["job"]["plan"]["steps"])
    st.write("**Integration targets (from STTM):**", ", ".join(state["job"]["plan"]["integration_targets"]))

exec_id = uuid.uuid4().hex[:6]
if st.button("Run Selected Steps", key=f"btn_run_{exec_id}"):
    if not state["job"].get("plan"):
        st.error("Please click **Generate Plan** first."); st.stop()
    sttm_df = load_sttm_excel(state["job"]["sttm"]); validate_sttm(sttm_df)
    for src in state["job"]["sources"]:
        name = src["name"]
        st.write(f"### Processing source: {name}")
        if "Landing" in state["job"]["plan"]["steps"]:
            with st.spinner(f"Writing landing for {name}..."):
                try:
                    df = pd.read_csv(io.BytesIO(src["payload_bytes"]))
                    state.setdefault("landing", {})[name] = df.to_csv(index=False)
                    location = write_local_df(df, "landing", name)
                    st.success(f"Landing written for **{name}** → {location}")
                    with st.expander(f"Preview Landing: {name}", expanded=False):
                        st.dataframe(df.head(20))
                except Exception as e:
                    st.error(f"Landing failed for {name}: {e}")
        dq_approved = True
        if "DQ" in state["job"]["plan"]["steps"]:
            try:
                df = pd.read_csv(io.StringIO(state["landing"][name]))
                default_pk = [c for c in df.columns if c.lower()=="id" or c.lower().endswith("_id")]
                dq = DQAgent(); rules_df = pd.DataFrame(dq.propose_rules(df, default_pk))
                st.markdown(f"**Data Quality — {name}** (rules used in this run)")
                st.data_editor(rules_df, key=f"dq_rules_{name}_{exec_id}", disabled=True)
                report = dq.run_checks(df, rules_df.to_dict("records"))
                st.write("DQ Summary:", report["summary"])
                failed = [r for r in report["results"] if not r["passed"]]
                if failed:
                    st.warning(f"Failed rules for {name}: {len(failed)}")
                    from utils.dq_rules import _failed_rows
                    for fr in failed:
                        st.markdown(f"- **{fr['rule']}** ({fr['severity']}) — failed rows: {fr['failed_count']}")
                        fail_df = _failed_rows(df, fr["rule"])
                        with st.expander(f"View failing rows: {fr['rule']}", expanded=False):
                            st.dataframe(fail_df.head(50))
                crit_fail = any((r["severity"]=="CRITICAL" and not r["passed"]) for r in report["results"])
                approve_default = state["job"]["dq"].get("approve_on_critical", False) or not crit_fail
                approve = st.checkbox(f"Approve to proceed despite CRITICAL failures for {name}", key=f"approve_{name}_{exec_id}", value=approve_default)
                dq_approved = bool(approve) or not crit_fail
                state.setdefault("dq_reports", {})[name] = {"report": report, "approved": bool(approve)}; save_state(state)
            except Exception as e:
                st.error(f"DQ failed for {name}: {e}")
        if "Integration" in state["job"]["plan"]["steps"]:
            if not dq_approved:
                st.warning(f"Skipping Integration for {name} due to unapproved CRITICAL DQ failures.")
            else:
                try:
                    df_land = pd.read_csv(io.StringIO(state["landing"][name]))
                    integ_targets = sorted(sttm_df[(sttm_df["Target Schema"].str.lower()=="integration") & (sttm_df["Source Table"].str.lower()==name.lower())]["Target Table"].unique().tolist())
                    for it in integ_targets:
                        plan = build_integration_plan(sttm_df, it)
                        ref_df_map = {}
                        for r in plan["refs"]:
                            csv_path = os.path.join("data","samples", f"{r['name']}.csv")
                            if os.path.exists(csv_path): ref_df_map[r["name"]] = pd.read_csv(csv_path)
                        transformer = TransformAgent()
                        integ_tmp = transformer.integrate(df_land, ref_df_map, {"left_on": plan["left_on"], "refs": plan["refs"]})
                        df_map = {("landing", name): integ_tmp}
                        for r in plan["refs"]:
                            nm = r["name"]
                            if nm in ref_df_map: df_map[("reference", nm)] = ref_df_map[nm]
                        integrated = project_columns(df_map, plan["projection"])
                        scd_type = get_scd_for_target(sttm_df, "integration", it, default_scd=state.get("job",{}).get("hints",{}).get("scd","SCD1"))
                        keys = get_keys_for_target(sttm_df, "integration", it) or [c for c in integrated.columns if c.lower().endswith("_id")]
                        path_exist = os.path.join("data","integration",f"{it}.csv")
                        existing = pd.read_csv(path_exist) if os.path.exists(path_exist) else None
                        final = TransformAgent().scd_load(existing, integrated, scd_type, keys)
                        location = write_local_df(final, "integration", it)
                        with st.expander(f"Preview Integration: {it}", expanded=False):
                            st.dataframe(final.head(20))
                        st.success(f"Integration written for **{it}** → {location}")
                        state.setdefault("integration", {})[it] = final.to_csv(index=False); save_state(state)
                except Exception as e:
                    st.error(f"Integration failed for {name}: {e}")
        if "DWH" in state["job"]["plan"]["steps"]:
            try:
                dwh_targets = build_dwh_targets(sttm_df)
                for dtgt in dwh_targets:
                    src_tables = sttm_df[(sttm_df["Target Schema"].str.lower()=="dwh") & (sttm_df["Target Table"].str.lower()==dtgt.lower())]["Source Table"].unique().tolist()
                    integ_name = next((t for t in src_tables if t in (state.get("integration",{}).keys())), None)
                    if not integ_name: continue
                    integ_df = pd.read_csv(io.StringIO(state["integration"][integ_name]))
                    scd_type = get_scd_for_target(sttm_df, "dwh", dtgt, default_scd=state.get("job",{}).get("hints",{}).get("scd","SCD1"))
                    keys = get_keys_for_target(sttm_df, "dwh", dtgt) or [c for c in integ_df.columns if c.lower().endswith("_id")]
                    path_exist = os.path.join("data","dwh",f"{dtgt}.csv")
                    existing = pd.read_csv(path_exist) if os.path.exists(path_exist) else None
                    final = TransformAgent().scd_load(existing, integ_df, scd_type, keys)
                    location = write_local_df(final, "dwh", dtgt)
                    with st.expander(f"Preview DWH: {dtgt}", expanded=False):
                        st.dataframe(final.head(20))
                    st.success(f"DWH written for **{dtgt}** → {location}")
                    state.setdefault("dwh", {})[dtgt] = final.to_csv(index=False); save_state(state)
            except Exception as e:
                st.error(f"DWH failed: {e}")

    if "Report" in state["job"]["plan"]["steps"]:
        try:
            context = {
                "run_id": state["run_id"],
                "when": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "config": {"sources": [s["name"] for s in state["job"]["sources"]],
                           "landing": "Local CSV", "integration": "Local CSV", "dwh": "Local CSV"},
                "landing_msg": "Completed for: " + ", ".join(state.get("landing", {}).keys() or []),
                "dq": {"summary": {"total": 0, "passed": 0, "failed": 0}},
                "integration_msg": "Completed for: " + ", ".join(state.get("integration", {}).keys() or []),
                "dwh_msg": "Completed for: " + ", ".join(state.get("dwh", {}).keys() or []),
            }
            if "dq_reports" in state:
                total = sum(len(v["report"]["results"]) for v in state["dq_reports"].values())
                failed = sum(sum(1 for r in v["report"]["results"] if not r["passed"]) for v in state["dq_reports"].values())
                passed = total - failed
                context["dq"]["summary"] = {"total": total, "passed": passed, "failed": failed}
            html = render_report_html("templates", context)
            out_dir = os.path.join("data","reports", state["run_id"]); paths = save_html_and_pdf(html, out_dir, "run_report")
            st.success(f"Report generated → {paths['html']}")
            st.markdown(f"[Open HTML]({paths['html']})")
        except Exception as e:
            st.error(f"Report failed: {e}")

st.markdown("---")
st.caption("LLM-enabled guided build. If OPENAI_API_KEY is set, I parse the chat to suggest steps and add sample sources. Then: Generate Plan → Run Selected Steps.")
