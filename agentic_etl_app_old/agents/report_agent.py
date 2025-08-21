from __future__ import annotations
import os
from typing import Dict, Any
from jinja2 import Environment, FileSystemLoader, select_autoescape

def render_report_html(template_dir: str, context: Dict[str, Any]) -> str:
    env = Environment(loader=FileSystemLoader(template_dir), autoescape=select_autoescape())
    tpl = env.get_template("report.html")
    return tpl.render(**context)

def save_html_and_pdf(html_str: str, out_dir: str, base_name: str) -> Dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, f"{base_name}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_str)
    pdf_path = ""
    try:
        from weasyprint import HTML  # optional
        pdf_path = os.path.join(out_dir, f"{base_name}.pdf")
        HTML(string=html_str).write_pdf(pdf_path)
    except Exception:
        pdf_path = ""
    return {"html": html_path, "pdf": pdf_path}
