from pathlib import Path
from fpdf import FPDF

base = Path(__file__).resolve().parent
md = (base.parent / "Guion_Exposicion_PC3.md").read_text(encoding="utf-8")


class PDF(FPDF):
    def footer(self):
        self.set_y(-12)
        self.set_font("ArialUni", "I", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, f"Guion PC3 - estudio | pag. {self.page_no()}", align="C")
        self.set_text_color(0, 0, 0)


pdf = PDF(orientation="P", unit="mm", format="A4")
pdf.set_margins(18, 18, 18)
pdf.set_auto_page_break(auto=True, margin=18)

font_path = Path(r"C:\Windows\Fonts\arial.ttf")
font_bold = Path(r"C:\Windows\Fonts\arialbd.ttf")
font_italic = Path(r"C:\Windows\Fonts\ariali.ttf")
pdf.add_font("ArialUni", "", str(font_path))
pdf.add_font("ArialUni", "B", str(font_bold if font_bold.exists() else font_path))
pdf.add_font("ArialUni", "I", str(font_italic if font_italic.exists() else font_path))
pdf.add_page()

usable = pdf.epw


def put(text, size=10, style="", color=(0, 0, 0), h=5):
    pdf.set_text_color(*color)
    pdf.set_font("ArialUni", style, size)
    # keep within page width
    pdf.multi_cell(usable, h, text)
    pdf.set_text_color(0, 0, 0)


for line in md.splitlines():
    raw = line.rstrip()
    if not raw.strip():
        pdf.ln(2)
        continue
    if raw.strip() == "---":
        pdf.ln(2)
        continue
    if raw.startswith("# "):
        put(raw[2:], size=15, style="B", h=8)
        pdf.ln(2)
    elif raw.startswith("## "):
        pdf.ln(2)
        put(raw[3:], size=12, style="B", h=7)
        pdf.ln(1)
    elif raw.startswith("### "):
        pdf.ln(1)
        put(raw[4:], size=11, style="B", color=(122, 31, 43), h=6)
        pdf.ln(1)
    elif raw.startswith("|") and set(raw.replace("|", "").replace("-", "").replace(":", "").strip()) == set():
        continue
    elif raw.startswith("|"):
        cells = [c.strip().replace("`", "") for c in raw.strip("|").split("|")]
        put(" | ".join(cells), size=8, h=4.2)
    elif raw.startswith("- ["):
        put("[ ] " + raw.split("]", 1)[-1].strip().replace("`", ""), size=10, h=5)
    elif raw.startswith("- "):
        put("- " + raw[2:].replace("`", "").replace("**", ""), size=10, h=5)
    else:
        t = raw.replace("**", "").replace("`", "").replace("*", "")
        put(t, size=10, h=5)

out = base / "Guion_Exposicion_PC3.pdf"
pdf.output(str(out))
print(f"OK {out}")
print(f"pages={pdf.page_no()} bytes={out.stat().st_size}")
