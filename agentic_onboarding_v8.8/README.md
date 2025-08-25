# Agentic Data Onboarding — v8.8
- Conversation-first, single greeting, left/right chat
- Immediate default STTM suggestion after dataset prompt
- Inline STTM upload in chat
- Outputs CSVs to data/landing, data/integration, data/dwh
- Run summary to reports/

## Run
pip install -r requirements.txt
streamlit run app.py

## Quick test
Say: onboard customers data → use suggested → no → end to end
You'll be asked BK/SCD if needed, then landing → integration → dwh will execute, saving CSVs.
