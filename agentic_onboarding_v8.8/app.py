import os, time, re
import streamlit as st
import pandas as pd
import numpy as np

from tools.sttm import (
    list_sttm_files, load_sttm_excel, extract_bk_from_business_logic,
    target_table_from_map, discover_datasets, normpath, suggest_sttm_for_dataset
)
from tools.llm import have_llm, infer_bk_from_profile, narrative, llm_route
from tools.intent import (
    parse_bk, parse_scd, parse_action, parse_dataset_from_text, parse_source_uri
)
from tools.connectors import write_sqlite, write_layer_csv
from agents import landing_agent, integration_agent, dwh_agent, reporting_agent

# ----------------------------- Page & Styles -----------------------------
st.set_page_config(page_title="Agentic Data Onboarding", page_icon="🧠", layout="wide")
st.markdown("""
<style>
/* user on the right */
[data-testid="stChatMessage-user"] { justify-content: flex-end; }
[data-testid="stChatMessage-user"] [data-testid="stMarkdownContainer"] {
  background: #eaf2ff; padding: 10px 14px; border-radius: 12px; max-width: 70%;
}
/* assistant on the left */
[data-testid="stChatMessage-assistant"] [data-testid="stMarkdownContainer"] {
  background: #f7f7fb; padding: 10px 14px; border-radius: 12px; max-width: 70%;
}
</style>
""", unsafe_allow_html=True)

# ----------------------- Ensure dirs & demo seeds ------------------------
DATA_DIRS = ["data/samples", "data/landing", "data/integration", "data/dwh", "reports", "sttm"]
for d in DATA_DIRS: os.makedirs(d, exist_ok=True)

def ensure_seed_samples_and_sttm():
    cust_path = "data/samples/customers.csv"
    ord_path = "data/samples/orders.csv"
    if not os.path.exists(cust_path):
        np.random.seed(42)
        customers = pd.DataFrame({
            "customer_id": range(1001, 1026),
            "email": [f"user{i}@example.com" for i in range(25)],
            "first_name": ["Ava","Ben","Cory","Diya","Eli","Faye","Gus","Hana","Ivan","Jia","Kai","Lia","Moe","Nia","Omar","Pia","Quin","Ria","Sol","Tia","Uma","Val","Wes","Xin","Yara"],
            "last_name":  ["Green","Young","Reed","Patel","Kim","Stone","Lopez","Hall","Cook","Ward","Hill","Adams","Baker","Cruz","Diaz","Ford","Gray","Hunt","King","Long","Moss","Ng","Owen","Pope","Rios"],
            "country": np.random.choice(["US","CA","UK"], size=25, p=[0.65,0.25,0.10]),
            "state":   np.random.choice(["NJ","NY","CA","TX","ON","BC"], size=25),
            "zip":     [str(10000+i) for i in range(25)],
            "birthdate": pd.to_datetime(np.random.choice(pd.date_range("1975-01-01","2005-12-31"), size=25)).date,
            "signup_ts": pd.to_datetime(np.random.choice(pd.date_range("2023-01-01","2025-08-01"), size=25)),
            "is_active": np.random.choice([True, False], size=25, p=[0.85, 0.15]),
        })
        customers.to_csv(cust_path, index=False)
    if not os.path.exists(ord_path):
        customers = pd.read_csv(cust_path)
        orders = pd.DataFrame({
            "order_id": range(5001, 5051),
            "customer_id": np.random.choice(customers["customer_id"], size=50),
            "order_ts": pd.to_datetime(np.random.choice(pd.date_range("2024-01-01","2025-08-01"), size=50)),
            "amount": np.round(np.random.uniform(10, 500, size=50), 2),
            "currency": np.random.choice(["USD","CAD","GBP"], size=50, p=[0.7,0.2,0.1]),
            "status":   np.random.choice(["NEW","PAID","CANCELLED","REFUNDED"], size=50, p=[0.5,0.35,0.1,0.05]),
        })
        orders.to_csv(ord_path, index=False)

    if not list_sttm_files():
        from tools.sttm import write_customer_sttm_xlsx, write_sales_sttm_xlsx
        write_customer_sttm_xlsx("sttm/customer_dim_sttm.xlsx")
        write_sales_sttm_xlsx("sttm/sales_fact_sttm.xlsx")

ensure_seed_samples_and_sttm()

# ----------------------------- Sidebar: Key -----------------------------
st.sidebar.header("Session")
key_state = st.session_state.get("key_pending", "")
key_input = st.sidebar.text_input("OpenAI API Key", value=key_state or "", type="password")
if st.sidebar.button("Save Key"):
    if key_input:
        os.environ["OPENAI_API_KEY"] = key_input
        st.session_state["key_pending"] = key_input
        st.sidebar.success("OpenAI key saved for this session.")
    else:
        st.sidebar.warning("Enter a key before saving.")
st.sidebar.info(f"LLM: {'**ON**' if have_llm() else '**OFF**'}")

# ---------------------------- Session state ----------------------------
if "state" not in st.session_state:
    st.session_state.state = {
        "dataset": None, "source_uri": None, "sttm_path": None, "run_mode": None,
        "skip_dq": False, "after_dq": None, "landing_loaded": False,
        "landing_table": None, "scd_integration": None, "bk_integration": [],
        "scd_dwh": None, "bk_dwh": [], "profile": None, "dq_results": None,
        "run_records": [], "suggested_sttm": None
    }
if "awaiting" not in st.session_state:
    st.session_state.awaiting = {
        "start_scope_choice": False, "awaiting_edit_sttm": False, "editing_sttm": False,
        "ask_use_suggested_sttm": False, "load_landing_confirmation": False,
        "bk_integration": False, "scd_integration": False, "bk_dwh": False, "scd_dwh": False,
        "another_dataset": False, "awaiting_upload": False,
        "confirm_integration": False, "confirm_dwh": False
    }
if "chat" not in st.session_state: st.session_state.chat=[]
if "welcome_emitted" not in st.session_state: st.session_state["welcome_emitted"]=False

LANDING_DIR="data/landing"; INTEGRATION_DIR="data/integration"; DWH_DIR="data/dwh"

# ----------------------------- Helpers -----------------------------
def queue_assistant(msg): st.session_state.chat.append(("assistant", msg))
def queue_user(msg): st.session_state.chat.append(("user", msg))
def narrate_now(msg):
    txt = narrative(st.session_state.chat, msg) if have_llm() else msg
    st.chat_message("assistant").write(txt); queue_assistant(txt)
def progress_steps(steps):
    ph = st.chat_message("assistant").empty()
    for s in steps:
        ph.write(f"_{s}…_"); time.sleep(0.25)
    return ph
def greet_once():
    if st.session_state["welcome_emitted"]: return
    d = discover_datasets()
    options = ", ".join([k.capitalize() for k in d.keys()]) if d else "any CSV you provide"
    queue_assistant(f"Hi! I can help with data onboarding. I can see datasets like {options}. What would you like to onboard?")
    st.session_state["welcome_emitted"] = True
def record(status, detail, rows=None):
    st.session_state.state["run_records"].append({
        "ts": pd.Timestamp.utcnow().isoformat(), "status": status, "detail": detail, "rows": rows
    })

def wants_scope(text: str) -> str | None:
    t = text.lower()
    if "landing" in t: return "landing"
    if "integration" in t: return "integration"
    if "warehouse" in t or "dwh" in t: return "dwh"
    if "end to end" in t or "end-to-end" in t or "e2e" in t: return "e2e"
    return None

def wants_profile(text: str) -> bool:
    t=text.lower(); return any(p in t for p in ["show profile","profiling result","profile result","view profile","profiling stats","show me the profile"])

def wants_dq(text: str) -> bool:
    t=text.lower(); return any(p in t for p in ["show dq","view dq","data quality","dq results","quality results","show data quality"])

# --------------------- Source helpers ---------------------
def _plural_guess(ds: str) -> str:
    if ds.endswith("s"): return ds
    if ds.endswith("y"): return ds[:-1] + "ies"
    return ds + "s"

def set_dataset_from_sttm_if_missing():
    S=st.session_state.state
    if S.get("dataset"): return
    try:
        sttm = load_sttm_excel(S["sttm_path"])
        cand = str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip()
        if cand:
            S["dataset"] = cand.lower()
    except Exception:
        pass

def normalize_source_uri():
    """Return a safe, normalized source URI string (file://...) or None."""
    S = st.session_state.state
    uri = S.get("source_uri")

    if uri and uri.lower().endswith((".xlsx", ".xls")) and ("sttm" in uri.lower()):
        uri = None

    if not uri:
        ds = (S.get("dataset") or "").lower().strip()
        if ds:
            cand = f"data/samples/{ds}.csv"
            if os.path.exists(cand):
                return "file://" + normpath(cand)
            cand2 = f"data/samples/{_plural_guess(ds)}.csv"
            if os.path.exists(cand2):
                return "file://" + normpath(cand2)
        if S.get("sttm_path"):
            sttm = load_sttm_excel(S["sttm_path"])
            try:
                src_table = str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip()
                cand3 = f"data/samples/{src_table}.csv"
                if os.path.exists(cand3):
                    return "file://" + normpath(cand3)
            except Exception:
                pass
        return None

    if "://" not in uri:
        return "file://" + normpath(uri)
    return uri

# -------------------------- Orchestrated runs --------------------------
def do_landing():
    S=st.session_state.state
    sttm=load_sttm_excel(S["sttm_path"])
    landing_table=str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip()
    S["landing_table"]=landing_table

    set_dataset_from_sttm_if_missing()
    src = normalize_source_uri()
    if not src:
        narrate_now("I don’t have a readable source file yet. Provide a source path (e.g., `file://data/samples/customers.csv`) or drop a CSV into `data/samples/` named after your dataset.")
        return

    steps=["Loading landing data"]
    if not S.get("skip_dq"): steps += ["Profiling sample", "Generating DQ rule suggestions"]
    progress_steps(steps)

    df,prof,dq_res,csv_path=landing_agent.land(src, "data/integration.db", landing_table, "data/landing", run_dq=(not S.get("skip_dq")))
    S["landing_loaded"]=True; S["profile"]=prof; S["dq_results"]=dq_res
    narrate_now(f"🛬 Landing complete → **{normpath(csv_path)}** ({len(df)} rows). {'Profiling & DQ ready (ask to view).' if not S.get('skip_dq') else 'DQ skipped as requested.'}")
    record("landing_complete", f"{csv_path}", rows=len(df))
    narrate_now("Shall I run **Integration** next? (yes/no)")
    st.session_state.awaiting.update({"confirm_integration": True, "confirm_dwh": False})

def do_integration():
    S=st.session_state.state
    sttm=load_sttm_excel(S["sttm_path"])
    landing_table=str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip()
    landing_csv=os.path.join("data/landing", f"{landing_table}.csv")
    if not os.path.exists(landing_csv):
        narrate_now("I don’t see a Landing output yet. Should I load Landing first? (yes/no)")
        st.session_state.awaiting["load_landing_confirmation"]=True; return

    df_landing=pd.read_csv(landing_csv)
    progress_steps(["Interpreting STTM for Integration","Deriving business rules","Applying transformations"])
    res_i=integration_agent.transform_to_integration(df_landing, S["sttm_path"])
    out=res_i["data"]; tgt_int=res_i["target_table"]
    merged=integration_agent.load_integration(None, out, S.get("scd_integration") or 1, S.get("bk_integration") or [])
    write_sqlite(merged, "data/integration.db", tgt_int, if_exists="replace")
    csv_out = write_layer_csv(merged, "data/integration", tgt_int)
    narrate_now(f"📦 Integration loaded → `{tgt_int}` with **{len(merged)}** rows at **{normpath(csv_out)}**.")
    record("integration_complete", f"{csv_out}", rows=len(merged))
    narrate_now("Proceed with **Data Warehouse**? (yes/no)")
    st.session_state.awaiting.update({"confirm_integration": False, "confirm_dwh": True})

def do_dwh():
    S=st.session_state.state
    sttm=load_sttm_excel(S["sttm_path"])
    int_tgt = target_table_from_map(sttm["int_map"])
    integ_csv=os.path.join("data/integration", f"{int_tgt}.csv")
    if not os.path.exists(integ_csv):
        dw_sources=set(sttm["dw_map"]["Source Schema"].str.lower().dropna().unique().tolist())
        if "landing" in dw_sources and "int" not in dw_sources:
            integ_df=pd.read_csv(os.path.join("data/landing", f"{sttm['int_map']['Source Table'].dropna().iloc[0]}.csv"))
        else:
            narrate_now(f"I can’t find Integration CSV `{normpath(integ_csv)}`. Please run Integration first."); return
    else:
        integ_df=pd.read_csv(integ_csv)

    progress_steps(["Reading DWH mappings","Applying DWH transformations"])
    res_d=dwh_agent.to_dwh(integ_df, S["sttm_path"]); dwh_in=res_d["data"]; tgt_dw=res_d["target_table"]
    merged_dw=dwh_agent.load_dwh(None, dwh_in, S.get("scd_dwh") or 1, S.get("bk_dwh") or [])
    write_sqlite(merged_dw, "data/warehouse.db", tgt_dw, if_exists="replace")
    csv_dw = write_layer_csv(merged_dw, "data/dwh", tgt_dw)
    narrate_now(f"✅ DWH loaded → `{tgt_dw}` with **{len(merged_dw)}** rows at **{normpath(csv_dw)}**.")
    record("dwh_complete", f"{csv_dw}", rows=len(merged_dw))

    rpt_csv, rpt_pdf, rpt_png = reporting_agent.summarize(st.session_state.state["run_records"], reports_dir="reports")
    with st.chat_message("assistant"):
        if rpt_png and os.path.exists(rpt_png): st.image(rpt_png, caption="Onboarding Run Summary")
        if rpt_csv or rpt_pdf:
            msg = "📝 Report saved:"
            if rpt_csv: msg += f" {normpath(rpt_csv)} (CSV)"
            if rpt_pdf: msg += f", {normpath(rpt_pdf)} (PDF)"
            st.success(msg)
    narrate_now(f"End-to-end load for **{S.get('dataset','your')}** data is complete. Would you like to onboard another dataset? (yes/no)")
    st.session_state.awaiting.update({"another_dataset": True, "confirm_integration": False, "confirm_dwh": False})

def ensure_prereqs_and_run(scope):
    S=st.session_state.state
    if not S.get("sttm_path"):
        ds = S.get("dataset") or "your dataset"
        sug = suggest_sttm_for_dataset(ds, 'sttm')
        if sug:
            S["suggested_sttm"]=sug
            narrate_now(f"I found an STTM for **{ds}**: `{normpath(sug)}`. Say **use suggested** to proceed or **use different** to upload here.")
            st.session_state.awaiting["ask_use_suggested_sttm"]=True; return
        else:
            st.session_state.awaiting["awaiting_upload"]=True
            narrate_now("Please upload an STTM Excel below to proceed."); return

    if scope in ("integration","dwh","e2e"):
        sttm=load_sttm_excel(S["sttm_path"])
        landing_table=str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip()
        landing_csv=os.path.join("data/landing", f"{landing_table}.csv")
        if not os.path.exists(landing_csv):
            narrate_now("Landing isn’t present yet. Should I load Landing first? (yes/no)")
            st.session_state.awaiting["load_landing_confirmation"]=True; return

    if scope in ("integration","e2e"):
        if not S.get("bk_integration"):
            sttm_all=load_sttm_excel(S["sttm_path"])["all"]
            bk_hint=extract_bk_from_business_logic(sttm_all)
            if bk_hint: narrate_now(f"I can use `{', '.join(bk_hint)}` as the business key. Confirm or provide another (e.g., `BK is customer_id`).")
            else:
                src = os.path.join("data/landing", f"{S.get('landing_table','')}.csv") if S.get("landing_table") else None
                df = pd.read_csv(src) if src and os.path.exists(src) else None
                if df is not None:
                    inferred=infer_bk_from_profile(df)
                    if inferred: narrate_now(f"Based on profiling, `{', '.join(inferred)}` looks like a good business key. Confirm or provide another.")
                else:
                    narrate_now("Please provide a business key for Integration (e.g., `BK is customer_id`).")
            st.session_state.awaiting["bk_integration"]=True; return
        if not S.get("scd_integration"):
            narrate_now("Which SCD type for Integration — **1** or **2**?")
            st.session_state.awaiting["scd_integration"]=True; return

    if scope in ("dwh","e2e"):
        if not S.get("bk_dwh"):
            narrate_now("What business key should I use in DWH? (say `same as integration` or `BK is customer_id`)")
            st.session_state.awaiting["bk_dwh"]=True; return
        if S.get("bk_dwh")==["same as integration"] or (S.get("bk_dwh") and S["bk_dwh"][0].lower()=="same as integration"):
            S["bk_dwh"]=S["bk_integration"]; narrate_now(f"Using Integration BK: {', '.join(S['bk_dwh'])}")
        if not S.get("scd_dwh"):
            narrate_now("Which SCD type for DWH — **1** or **2**?"); st.session_state.awaiting["scd_dwh"]=True; return

    if scope=="landing": do_landing()
    elif scope=="integration": do_integration()
    elif scope=="dwh": do_dwh()
    elif scope=="e2e":
        if not S.get("landing_loaded"): do_landing()
        do_integration(); do_dwh()

# ------------------------ Inline uploader -----------------------
def render_inline_uploader():
    with st.chat_message("assistant"):
        st.write("Upload your STTM Excel here, then click **Use this STTM**.")
        upl = st.file_uploader("Upload STTM (xlsx)", type=["xlsx"], key="inline_sttm_uploader")
        if upl is not None:
            buf_path = f"sttm/uploaded_{int(time.time())}.xlsx"
            with open(buf_path, "wb") as f: f.write(upl.read())
            st.session_state.state["sttm_path"]=buf_path
            st.session_state.awaiting.update({"ask_use_suggested_sttm": False, "awaiting_upload": False})
            st.success(f"Ready to use: {normpath(buf_path)}")
        if st.button("Use this STTM"):
            if st.session_state.state.get("sttm_path"):
                st.session_state.awaiting.update({
                    "ask_use_suggested_sttm": False,
                    "awaiting_upload": False,
                    "awaiting_edit_sttm": True
                })
                narrate_now("Got it. Would you like to review/edit the STTM before we proceed? (yes/no)")
            else:
                st.warning("Please upload a file first.")

# ------------------------------ Chat render ------------------------------
def replay():
    for role, txt in st.session_state.chat:
        st.chat_message(role).write(txt)

def start():
    if not st.session_state["welcome_emitted"]:
        greet_once()
    replay()
    if st.session_state.awaiting.get("awaiting_upload"):
        render_inline_uploader()
start()

# ------------------------------- Chat loop -------------------------------
user_text = st.chat_input("How can I help with data onboarding?")
if user_text:
    st.chat_message("user").write(user_text); queue_user(user_text)
    S=st.session_state.state; A=st.session_state.awaiting
    handled=False; t=user_text.lower().strip()

    # EARLY: strong "onboard ..." intent => fix "does nothing"
    onboard_ds = parse_dataset_from_text(user_text)
    if onboard_ds:
        S["dataset"] = onboard_ds
        act_hint = parse_action(user_text) or {}
        if act_hint.get("action"):
            S["run_mode"] = act_hint["action"]
        if not S.get("sttm_path"):
            sug = suggest_sttm_for_dataset(S["dataset"], "sttm")
            if sug:
                S["suggested_sttm"] = sug
                narrate_now(f"I found an STTM for **{S['dataset']}**: `{normpath(sug)}`. Say **use suggested** or **use different** to upload.")
                A["ask_use_suggested_sttm"] = True
            else:
                A["awaiting_upload"] = True
                narrate_now("Please upload an STTM Excel here, then say **use this STTM**.")
        handled = True

    # Convenience: "file is available in <path>"
    if not handled and "file is available in" in t:
        p = t.split("file is available in",1)[1].strip().strip(".").strip("`").strip()
        if p and os.path.exists(p):
            S["source_uri"]=p
            narrate_now(f"Got it. I’ll use `{normpath(p)}` as the source.")
            handled=True

    # reset
    if not handled and t in {"reset","restart"}:
        st.session_state.state = {
            "dataset": None,"source_uri": None,"sttm_path": None,"run_mode": None,"skip_dq": False,"after_dq": None,
            "landing_loaded": False,"landing_table": None,"scd_integration": None,"bk_integration": [],
            "scd_dwh": None,"bk_dwh": [],"profile": None,"dq_results": None, "run_records": [], "suggested_sttm": None
        }
        st.session_state.awaiting = {
            "start_scope_choice": False,"awaiting_edit_sttm": False,"editing_sttm": False,
            "ask_use_suggested_sttm": False,"load_landing_confirmation": False,"bk_integration": False,"scd_integration": False,
            "bk_dwh": False,"scd_dwh": False,"another_dataset": False,"awaiting_upload": False,
            "confirm_integration": False, "confirm_dwh": False
        }
        narrate_now("Okay, I’ve reset the session. What dataset would you like to onboard?"); handled=True

    # LLM router (optional)
    if not handled:
        route = llm_route(user_text, st.session_state.chat) if have_llm() else None
        if route:
            if route.get("dataset"): S["dataset"] = route["dataset"]
            if route.get("source_uri"):
                su = route["source_uri"]
                if not (su.lower().endswith((".xlsx",".xls")) and "sttm" in su.lower()):
                    S["source_uri"]=su
            if route.get("scope"): S["run_mode"]=route["scope"]
            if route.get("sttm_choice")=="suggested": A["ask_use_suggested_sttm"]=True
            if route.get("sttm_choice")=="different": A["awaiting_upload"]=True
            if route.get("bk"): S["bk_integration"]=route["bk"]; S["bk_dwh"]=route["bk"]
            if route.get("scd_integration"): S["scd_integration"]=route["scd_integration"]
            if route.get("scd_dwh"): S["scd_dwh"]=route["scd_dwh"]

    # Regex fallback for dataset & source
    if not handled:
        ds = parse_dataset_from_text(t)
        if ds: S["dataset"]=ds
        src = parse_source_uri(t)
        if src and not (src.lower().endswith((".xlsx",".xls")) and "sttm" in src.lower()):
            S["source_uri"]=src

    # Action fallback
    if not handled:
        act = parse_action(user_text) or {}
        if "run_dq" in act: S["skip_dq"]=not act["run_dq"]
        if "after_dq" in act: S["after_dq"]=act["after_dq"]
        if act.get("action"): S["run_mode"]=act["action"]

    # Accept suggested STTM
    if not handled and A.get("ask_use_suggested_sttm"):
        t_clean = re.sub(r"[!?.]", "", t)
        hinted = parse_action(user_text) or {}
        hinted_scope = hinted.get("action")
        if any(x in t_clean for x in ["use suggested","select suggested","yes","y","ok","okay","yep","proceed with suggested","go with suggested","use this sttm","use this"]):
            if S.get("suggested_sttm"):
                S["sttm_path"]=S["suggested_sttm"]
                # infer dataset from STTM if missing
                if not S.get("dataset"):
                    try:
                        sttm = load_sttm_excel(S["sttm_path"])
                        S["dataset"] = str(sttm["int_map"]["Source Table"].dropna().iloc[0]).strip().lower()
                    except Exception:
                        pass
                A["ask_use_suggested_sttm"]=False
                narrate_now(f"Using **{normpath(S['sttm_path'])}**.")
                if hinted_scope: S["run_mode"]=hinted_scope
                A["awaiting_edit_sttm"]=True
                narrate_now("Would you like to **review/edit** the STTM before we proceed? (yes/no)")
                handled=True
            else:
                A.update({"ask_use_suggested_sttm": False, "awaiting_upload": True})
                narrate_now("I lost track of the suggested file. Please upload your STTM below."); handled=True
        elif any(x in t_clean for x in ["use different","different","upload","another","pick different","choose another"]):
            A.update({"ask_use_suggested_sttm": False, "awaiting_upload": True})
            narrate_now("Upload your STTM below, then click **Use this STTM**."); handled=True

    # STTM preview decision (editor-first)
    if not handled and (A.get("awaiting_edit_sttm") or ("edit sttm" in t) or ("review sttm" in t) or ("preview sttm" in t)):
        A["editing_sttm"]=True; A["awaiting_edit_sttm"]=False
        narrate_now("Opening the STTM editor.")
        handled=True

    # Inline STTM editor (no auto-continue while open)
    if A.get("editing_sttm"):
        with st.chat_message("assistant"):
            st.write("Edit your STTM mapping below and choose an action.")
            sttm = load_sttm_excel(S["sttm_path"])["all"]
            edited = st.data_editor(sttm, use_container_width=True, num_rows="dynamic", key="sttm_editor_inline")

            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Save STTM", key="save_sttm_inline"):
                    ts=pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
                    base=os.path.basename(S["sttm_path"]).replace(".xlsx","")
                    newp=f"sttm/{base}_edited_{ts}.xlsx"
                    with pd.ExcelWriter(newp, engine="openpyxl") as w:
                        edited.to_excel(w, index=False, sheet_name="STTM")
                    S["sttm_path"]=newp
                    st.success(f"Saved edits to {normpath(newp)}")

            with c2:
                if st.button("Save & Proceed", key="save_and_proceed_inline"):
                    ts=pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
                    base=os.path.basename(S["sttm_path"]).replace(".xlsx","")
                    newp=f"sttm/{base}_edited_{ts}.xlsx"
                    with pd.ExcelWriter(newp, engine="openpyxl") as w:
                        edited.to_excel(w, index=False, sheet_name="STTM")
                    S["sttm_path"]=newp
                    A.update({"editing_sttm": False})
                    scope = S.get("run_mode") or "landing"
                    narrate_now(f"Saved to **{normpath(newp)}**. Proceeding.")
                    ensure_prereqs_and_run(scope)

            with c3:
                if st.button("Proceed without Saving", key="proceed_without_saving_inline"):
                    A.update({"editing_sttm": False})
                    scope = S.get("run_mode") or "landing"
                    narrate_now("Proceeding without saving inline changes.")
                    ensure_prereqs_and_run(scope)
        st.stop()

    # Uploader (only when explicitly requested)
    if st.session_state.awaiting.get("awaiting_upload"):
        render_inline_uploader()
        st.stop()

    # Natural profile/DQ preview
    if not handled and wants_profile(user_text) and S.get("profile"):
        with st.chat_message("assistant"):
            st.write("Here’s the latest **profiling** result:")
            st.json(S["profile"])
        handled=True
    if not handled and wants_dq(user_text) and (S.get("dq_results") is not None):
        with st.chat_message("assistant"):
            st.write("Here are the **Data Quality** results:")
            st.json(S["dq_results"])
        handled=True

    # BK / SCD capture
    if not handled:
        bk = parse_bk(user_text)
        if bk:
            S["bk_integration"]=bk; S["bk_dwh"]=bk; narrate_now(f"I’ll use {', '.join(bk)} as the business key.")
            handled=True
        scd = parse_scd(user_text)
        if scd:
            if not S.get("scd_integration"): S["scd_integration"]=scd
            if not S.get("scd_dwh"): S["scd_dwh"]=scd
            narrate_now(f"SCD set to type {scd}."); handled=True

    # Step confirmations
    if not handled and t in {"yes","y","yeah","yep","confirm","proceed","do it","go ahead","sure","ok","okay","yes please"}:
        if st.session_state.awaiting.get("load_landing_confirmation"):
            st.session_state.awaiting["load_landing_confirmation"]=False; do_landing(); handled=True
        elif st.session_state.awaiting.get("confirm_integration"):
            st.session_state.awaiting["confirm_integration"]=False; ensure_prereqs_and_run("integration"); handled=True
        elif st.session_state.awaiting.get("confirm_dwh"):
            st.session_state.awaiting["confirm_dwh"]=False; ensure_prereqs_and_run("dwh"); handled=True
        elif st.session_state.awaiting.get("another_dataset"):
            st.session_state.awaiting["another_dataset"]=False
            st.session_state.state = {
                "dataset": None,"source_uri": None,"sttm_path": None,"run_mode": None,"skip_dq": False,"after_dq": None,
                "landing_loaded": False,"landing_table": None,"scd_integration": None,"bk_integration": [],
                "scd_dwh": None,"bk_dwh": [],"profile": None,"dq_results": None, "run_records": [], "suggested_sttm": None
            }
            narrate_now("Great — what dataset should we onboard next?")
            handled=True

    if not handled and t in {"no","n","nope","not now","later"}:
        if st.session_state.awaiting.get("another_dataset"):
            st.session_state.awaiting["another_dataset"]=False; narrate_now("All set. I’m here if you need anything else.")
            handled=True

    # Manual scope selection
    if not handled and (st.session_state.awaiting.get("start_scope_choice") or S.get("run_mode")):
        if "landing" in t: st.session_state.awaiting["start_scope_choice"]=False; ensure_prereqs_and_run("landing"); handled=True
        elif "integration" in t: st.session_state.awaiting["start_scope_choice"]=False; ensure_prereqs_and_run("integration"); handled=True
        elif "warehouse" in t or "dwh" in t: st.session_state.awaiting["start_scope_choice"]=False; ensure_prereqs_and_run("dwh"); handled=True
        elif any(k in t for k in ["end-to-end","end to end","e2e","all the way","full pipeline"]) or S.get("run_mode")=="e2e":
            st.session_state.awaiting["start_scope_choice"]=False; ensure_prereqs_and_run("e2e"); handled=True

    # Auto-continue only when not editing
    if (not st.session_state.awaiting.get("awaiting_edit_sttm")) and (not st.session_state.awaiting.get("editing_sttm")):
        if not handled and S.get("dataset") and S.get("sttm_path"):
            scope = S.get("run_mode") or ("landing" if not S.get("landing_loaded") else "integration")
            ensure_prereqs_and_run(scope)
