import pdfplumber

pdf_path = "YOUR_FILE_NAME_HERE.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages, start=1):
        text = page.extract_text()
        print(f"\n--- PAGE {i} ---\n")
        print(text[:4000] if text else "NO TEXT FOUND")