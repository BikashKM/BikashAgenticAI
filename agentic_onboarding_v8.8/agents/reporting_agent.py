import os
import pandas as pd

# Optional plotting deps (graceful fallback if missing)
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except Exception:
    HAS_MPL = False

def _safe_png(chart_data, png_path):
    if not HAS_MPL:
        return None
    try:
        steps = [r.get("status","step") for r in chart_data]
        values = [r.get("rows", 0) or 0 for r in chart_data]
        plt.figure(figsize=(8, 4.5))
        plt.bar(steps, values)
        plt.title("Onboarding Run Summary")
        plt.xlabel("Step"); plt.ylabel("Row count")
        plt.tight_layout()
        plt.savefig(png_path, dpi=160); plt.close()
        return png_path
    except Exception:
        return None

def _safe_pdf(chart_png, table_rows, pdf_path):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import inch
        from reportlab.platypus import Table, TableStyle
        from reportlab.lib import colors

        c = canvas.Canvas(pdf_path, pagesize=A4)
        width, height = A4
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, height - 40, "Onboarding Run Summary")

        if chart_png and os.path.exists(chart_png):
            c.drawImage(chart_png, 40, height - 340, width=520, height=260, preserveAspectRatio=True, mask='auto')

        data = [["Timestamp", "Status", "Detail", "Rows"]]
        for r in table_rows:
            data.append([r.get("ts",""), r.get("status",""), r.get("detail",""), str(r.get("rows",""))])

        tbl = Table(data, colWidths=[140, 110, 220, 50])
        tbl.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0), colors.lightgrey),
            ('GRID',(0,0),(-1,-1), 0.25, colors.grey),
            ('FONT',(0,0),(-1,0),'Helvetica-Bold'),
            ('FONTSIZE',(0,0),(-1,-1),9),
            ('ALIGN',(-1,1),(-1,-1),'RIGHT')
        ]))
        w, h = tbl.wrapOn(c, width-80, 200)
        tbl.drawOn(c, 40, 80)

        c.showPage(); c.save()
        return pdf_path
    except Exception:
        return None

def summarize(run_records, reports_dir="reports"):
    os.makedirs(reports_dir, exist_ok=True)
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(reports_dir, f"run_summary_{ts}.csv")
    pdf_path = os.path.join(reports_dir, f"run_summary_{ts}.pdf")
    png_path = os.path.join(reports_dir, f"run_summary_{ts}.png")

    df = pd.DataFrame(run_records)
    df.to_csv(csv_path, index=False)

    png_done = _safe_png(run_records, png_path)
    pdf_done = _safe_pdf(png_done, run_records, pdf_path)

    return csv_path, pdf_done, png_done
