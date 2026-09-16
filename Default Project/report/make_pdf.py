"""Render report/REPORT.md to report/REPORT.pdf (no external deps besides fpdf2)."""
from fpdf import FPDF
from pathlib import Path

SRC = Path(__file__).parent / "REPORT.md"
DST = Path(__file__).parent / "REPORT.pdf"


def clean(t: str) -> str:
    return (t.replace("\u2014", "-").replace("\u2013", "-")
             .replace("\u2022", "-").replace("\u2192", "->")
             .replace("\u00d7", "x").encode("latin-1", "replace").decode("latin-1"))


class PDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 8, "U1T01: PostgreSQL CDC & ClickHouse", align="R")
            self.ln(10)
            self.set_text_color(0, 0, 0)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")


def main():
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(True, margin=20)
    pdf.add_page()
    lines = SRC.read_text().splitlines()
    in_code = False
    for raw in lines:
        line = raw.rstrip()
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            pdf.set_x(pdf.l_margin)
            pdf.set_font("Courier", "", 8)
            pdf.set_fill_color(245, 245, 245)
            pdf.multi_cell(0, 4.5, clean("  " + line[:150]), fill=True)
            continue
        s = line.strip()
        if not s:
            pdf.ln(3)
            continue
        pdf.set_x(pdf.l_margin)
        if s.startswith("# "):
            pdf.set_font("Helvetica", "B", 20)
            pdf.multi_cell(0, 10, clean(s[2:]))
            pdf.ln(2)
        elif s.startswith("## "):
            pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(20, 80, 110)
            pdf.multi_cell(0, 8, clean(s[3:]))
            pdf.set_text_color(0, 0, 0)
            pdf.ln(1)
        elif s.startswith("### "):
            pdf.set_font("Helvetica", "B", 11)
            pdf.multi_cell(0, 7, clean(s[4:]))
        elif s.startswith("|"):
            pdf.set_font("Courier", "", 7.5)
            pdf.multi_cell(0, 4.5, clean(s[:170]))
        elif s.startswith(("- ", "* ")):
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5.5, clean("-  " + s[2:]))
        elif s[0].isdigit() and len(s) > 2 and s[1:3] in (". ", ") "):
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5.5, clean(s))
        else:
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 5.5, clean(s))
    pdf.output(str(DST))
    print(f"wrote {DST} ({DST.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
