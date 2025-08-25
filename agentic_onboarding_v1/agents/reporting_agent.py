
import os, datetime as dt, pandas as pd
from typing import Dict, Any, List

def write_html_report(path: str, context: Dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    html = ["<html><head><meta charset='utf-8'><title>Run Report</title>"]
    html.append("<style>body{font-family:Inter,Arial;padding:20px} .ok{color:green} .bad{color:#b00} code{background:#f6f6f6;padding:2px 4px}</style>")
    html.append("</head><body>")
    html.append(f"<h2>Data Onboarding Run Report</h2>")
    html.append(f"<p><b>Run ID:</b> {context.get('run_id')}</p>")
    html.append(f"<p><b>Timestamp (UTC):</b> {dt.datetime.utcnow()}</p>")
    for sec in ["landing","dq","integration","dwh"]:
        html.append(f"<h3>{sec.upper()}</h3>")
        secdata = context.get(sec, {})
        html.append("<pre><code>")
        html.append(str(secdata))
        html.append("</code></pre>")
    html.append("</body></html>")
    open(path, "w").write("\n".join(html))
    return path
