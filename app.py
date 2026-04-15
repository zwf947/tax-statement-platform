from __future__ import annotations

import csv
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import fitz  # PyMuPDF
import numpy as np
import pandas as pd
import pdfplumber
from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from PIL import Image

# Optional OCR fallback
try:
    import easyocr  # type: ignore
    EASYOCR_AVAILABLE = True
except Exception:
    easyocr = None
    EASYOCR_AVAILABLE = False


BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
EXPORTS_DIR = BASE_DIR / "exports"
DB_PATH = BASE_DIR / "transactions.db"

UPLOADS_DIR.mkdir(exist_ok=True)
EXPORTS_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.secret_key = "replace-this-with-a-better-secret-key"

ocr_reader = easyocr.Reader(["en"], gpu=False) if EASYOCR_AVAILABLE else None


CATEGORY_RULES = {
    "Payroll": ["payroll", "adp", "gusto", "paychex"],
    "Rent": ["rent", "landlord", "property management", "lease"],
    "Utilities": ["dte", "consumers energy", "water", "internet", "comcast", "verizon", "att"],

    "Software / SaaS": [
        "microsoft", "google", "aws", "openai", "adobe", "dropbox", "zoom", "slack", "notion",
        "canva", "mailchimp", "capcut", "distrokid", "tunecore", "cd baby", "landr", "splice"
    ],

    "Fuel": ["shell", "bp", "exxon", "mobil", "speedway", "marathon", "sunoco", "gas"],

    "Meals": [
        "doordash", "uber eats", "grubhub", "restaurant", "cafe", "coffee", "mcdonald",
        "subway", "chipotle", "panera", "pizza", "burger", "diner", "taco"
    ],

    "Travel": ["delta", "united", "american airlines", "airbnb", "marriott", "hilton", "uber", "lyft"],
    "Office Supplies": ["staples", "office depot", "amazon", "amzn"],
    "Taxes & Fees": ["irs", "state tax", "city tax", "tax payment", "license fee"],
    "Transfer": ["transfer", "zelle", "venmo", "cash app", "ach transfer", "paypal"],
    "Loan Payment": ["loan", "capital one auto", "ford credit", "mortgage"],
    "Revenue": ["deposit", "payment received", "stripe", "square", "shopify payout", "ach credit", "refund", "interest"],
    "Insurance": ["progressive", "state farm", "geico", "allstate"],
    "Phone / Internet": ["t mobile", "tmobile", "at&t", "att", "verizon", "xfinity"],
    "Auto Expense": ["oil change", "repair", "service center", "tire", "midas", "firestone"],
    "Groceries": ["walmart", "kroger", "meijer", "target", "aldi", "costco", "whole foods"],
    "Subscriptions": ["netflix", "spotify", "apple.com/bill", "google", "youtube", "hulu", "adobe"],

    "Advertising / Promotion": [
        "facebook ads", "meta ads", "instagram ads", "google ads", "youtube ads",
        "tiktok ads", "promo", "promotion", "advertising", "ad campaign", "boosted post"
    ],

    "Music Marketing": [
        "playlist push", "submit hub", "submithub", "groover", "songtools", "hypedit",
        "feature fm", "feature.fm", "toneden", "music marketing", "publicity"
    ],

    "Studio / Recording": [
        "recording studio", "studio time", "mixing", "mastering", "engineer", "producer fee",
        "tracking", "session", "recording", "master", "mix revision"
    ],

    "Music Equipment": [
        "guitar center", "sweetwater", "reverb", "musicians friend", "musician's friend",
        "sam ash", "instrument", "microphone", "mic", "audio interface", "speaker",
        "monitor", "headphones", "keyboard", "guitar", "bass", "drum", "pedal", "amp"
    ],

    "Live Performance": [
        "venue", "ticket fee", "backline", "foh", "front of house", "monitor engineer",
        "sound engineer", "live nation", "show expense", "performance fee", "gig"
    ],

    "Tour / Band Travel": [
        "tour", "touring", "hotel", "motel", "lodging", "van rental", "uhaul", "u-haul",
        "airfare", "flight", "gas station", "travelodge"
    ],

    "Merchandise": [
        "merch", "merchandise", "t shirt", "t-shirt", "hoodie", "screen printing",
        "printful", "printify", "stickers", "posters", "vinyl pressing", "cd pressing"
    ],

    "Photo / Video": [
        "photographer", "videographer", "video shoot", "photo shoot", "camera rental",
        "editing", "visualizer", "lyric video", "music video", "content shoot"
    ],

    "Distribution / Publishing": [
        "distrokid", "tunecore", "cd baby", "songtrust", "ascap", "bmi", "sesac",
        "distribution", "publishing", "royalty", "mechanical license"
    ],

    "Rehearsal / Practice": [
        "rehearsal", "practice space", "lockout", "band room", "jam space"
    ],

    "Wardrobe / Stagewear": [
        "stage clothes", "wardrobe", "costume", "boots", "jacket", "outfit", "mask", "stagewear"
    ],
}

ALL_CATEGORIES = sorted(list(CATEGORY_RULES.keys()) + [
    "Band Member Payment",
    "Session Musician",
    "Artwork / Design",
    "Website / Domain",
    "Uncategorized Expense",
    "Uncategorized",
])

DATE_PATTERNS = [
    r"\d{1,2}/\d{1,2}/\d{2,4}",
    r"\d{1,2}/\d{1,2}",
    r"\d{4}-\d{2}-\d{2}",
]

AMOUNT_PATTERN = r"[-(]?\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})[)]?"

STATEMENT_NOISE_TERMS = [
    "date description amount",
    "beginning balance",
    "ending balance",
    "account number",
    "page ",
    "member fdic",
    "daily balance",
    "debits",
    "credits",
    "withdrawals",
    "deposits",
    "transactions",
    "statement period",
    "balance forward",
    "check number",
    "description withdrawals deposits balance",
    "continued on next page",
    "account summary",
    "total fees",
    "interest paid",
    "customer service",
    "balance summary",
    "total deposits",
    "total withdrawals",
]

RESTAURANT_HINTS = [
    "restaurant", "cafe", "coffee", "pizza", "burger", "grill", "bar", "bistro",
    "tavern", "diner", "bbq", "steakhouse", "kitchen", "eatery", "wings",
    "subs", "tacos", "sushi", "noodles", "bakery", "donuts", "smoothie",
    "juice", "espresso", "pub", "food", "saloon", "cantina", "brewery",
    "coney", "pizzeria", "taqueria"
]

RESTAURANT_BRANDS = [
    "starbucks", "mcdonald", "burger king", "wendys", "taco bell", "subway",
    "chipotle", "panera", "dunkin", "dunkin donuts", "tim hortons", "kfc",
    "arbys", "dominos", "pizza hut", "little caesars", "papa johns",
    "doordash", "uber eats", "grubhub", "jimmy johns", "five guys",
    "buffalo wild wings", "olive garden", "texas roadhouse", "applebees",
    "chilis", "outback", "red robin", "ihop", "dennys", "coney island",
    "savvy sliders", "qdoba", "jersey mikes", "potbelly", "culvers",
    "white castle", "shake shack", "biggby", "popeyes", "checkers",
    "rallys", "jets pizza", "hungry howies", "penn station"
]

MERCHANT_PREFIXES_TO_STRIP = [
    "pos purchase",
    "pos debit",
    "debit card purchase",
    "debit purchase",
    "card purchase",
    "purchase",
    "dbt purch",
    "checkcard",
    "visa purchase",
    "mc purchase",
    "withdrawal",
    "recurring",
    "ach debit",
    "ach",
    "sq *",
    "sq ",
    "tst*",
    "tst ",
    "paypal *",
    "paypal ",
    "pp*",
    "dd *",
    "dd ",
]

MERCHANT_CATEGORY_MAP = {
    "jims coney island": "Meals",
    "jim s coney island": "Meals",
    "savvy sliders": "Meals",
    "panera bread": "Meals",
    "starbucks": "Meals",
    "doordash": "Meals",
    "uber eats": "Meals",
    "grubhub": "Meals",
    "mcdonald": "Meals",
    "chipotle": "Meals",
    "subway": "Meals",
    "pizza hut": "Meals",
    "little caesars": "Meals",
    "dominos": "Meals",
    "dunkin": "Meals",
    "tim hortons": "Meals",
}


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                txn_date TEXT,
                raw_description TEXT,
                clean_description TEXT,
                amount REAL,
                category TEXT,
                parse_method TEXT,
                confidence REAL,
                review_flag INTEGER,
                source_file TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def clean_description(text: str) -> str:
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s/&.\-*]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_merchant_name(description: str) -> str:
    text = clean_description(description)

    for prefix in MERCHANT_PREFIXES_TO_STRIP:
        if text.startswith(prefix):
            text = text[len(prefix):].strip()

    text = re.sub(r"\b\d{4,}\b", " ", text)
    text = re.sub(r"\b(?:com|help|www|online|web|auth|debit|card|visa|mc)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def looks_like_restaurant(description: str) -> bool:
    merchant = normalize_merchant_name(description)

    for brand in RESTAURANT_BRANDS:
        if brand in merchant:
            return True

    for hint in RESTAURANT_HINTS:
        if hint in merchant:
            return True

    return False


def normalize_amount(amount_text: str) -> Optional[float]:
    if not amount_text:
        return None

    text = amount_text.strip().replace("$", "").replace(",", "")
    text = text.replace(" ", "")

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    if text.startswith("-"):
        negative = True
        text = text[1:]

    try:
        value = float(text)
        return -value if negative else value
    except ValueError:
        return None


def parse_date(date_text: str) -> Optional[str]:
    if not date_text:
        return None

    date_text = date_text.strip()

    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m/%d", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_text, fmt)
            if fmt == "%m/%d":
                dt = dt.replace(year=datetime.now().year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def looks_like_date(value: str) -> bool:
    if not value:
        return False
    value = value.strip()
    return any(re.search(rf"^{pattern}$|{pattern}", value) for pattern in DATE_PATTERNS)


def looks_like_amount(value: str) -> bool:
    if not value:
        return False
    return re.search(AMOUNT_PATTERN, value.strip()) is not None


def detect_bank(text: str) -> str:
    lower = text.lower()

    if "chase" in lower:
        return "chase"
    if "bank of america" in lower:
        return "bank_of_america"
    if "capital one" in lower:
        return "capital_one"
    if "wells fargo" in lower:
        return "wells_fargo"
    if "citibank" in lower or " citi " in f" {lower} ":
        return "citi"
    if "pnc" in lower:
        return "pnc"
    if "fifth third" in lower:
        return "fifth_third"
    return "unknown"


def categorize_transaction(description: str, amount: float) -> str:
    desc = clean_description(description)
    merchant = normalize_merchant_name(description)

    for merchant_name, category in MERCHANT_CATEGORY_MAP.items():
        if merchant_name in merchant:
            return category

    if looks_like_restaurant(description):
        return "Meals"

    for category, keywords in CATEGORY_RULES.items():
        for keyword in keywords:
            if keyword in desc or keyword in merchant:
                return category

    if amount > 0:
        return "Revenue"
    if amount < 0:
        return "Uncategorized Expense"
    return "Uncategorized"


def compute_confidence(
    description: str,
    amount: Optional[float],
    date_text: Optional[str],
    category: str,
    parse_method: str,
) -> float:
    score = 0.0

    if date_text:
        score += 0.30
    if amount is not None:
        score += 0.30
    if description and len(description.strip()) >= 3:
        score += 0.20
    if "uncategorized" not in category.lower():
        score += 0.15

    if parse_method == "csv":
        score += 0.05
    elif parse_method == "pdf-table":
        score += 0.03
    elif parse_method == "pdf-text":
        score += 0.02
    elif parse_method == "pdf-bank":
        score += 0.04
    elif parse_method == "pdf-ocr":
        score += 0.01

    return round(min(score, 1.0), 2)


def needs_review(
    date_text: Optional[str],
    description: str,
    amount: Optional[float],
    category: str,
    confidence: float,
) -> bool:
    if not date_text:
        return True
    if not description or len(description.strip()) < 3:
        return True
    if amount is None:
        return True
    if confidence < 0.75:
        return True
    if "uncategorized" in category.lower():
        return True
    return False


def is_probable_header_or_noise(text: str) -> bool:
    lowered = clean_description(text)
    return any(term in lowered for term in STATEMENT_NOISE_TERMS)


def save_transactions(rows: List[Dict[str, Any]], source_file: str) -> int:
    now = datetime.utcnow().isoformat()
    inserted_count = 0

    with get_db() as conn:
        for row in rows:
            txn_date = row["txn_date"]
            raw_description = row["raw_description"]
            clean_desc = clean_description(raw_description)
            amount = row["amount"]

            existing = conn.execute(
                """
                SELECT id
                FROM transactions
                WHERE txn_date = ?
                  AND clean_description = ?
                  AND amount = ?
                  AND source_file = ?
                """,
                (txn_date, clean_desc, amount, source_file),
            ).fetchone()

            if existing:
                continue

            conn.execute(
                """
                INSERT INTO transactions (
                    txn_date,
                    raw_description,
                    clean_description,
                    amount,
                    category,
                    parse_method,
                    confidence,
                    review_flag,
                    source_file,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    txn_date,
                    raw_description,
                    clean_desc,
                    amount,
                    row["category"],
                    row["parse_method"],
                    row["confidence"],
                    int(row["review_flag"]),
                    source_file,
                    now,
                ),
            )
            inserted_count += 1

        conn.commit()

    return inserted_count


def load_all_transactions() -> List[sqlite3.Row]:
    with get_db() as conn:
        return conn.execute(
            """
            SELECT *
            FROM transactions
            ORDER BY review_flag DESC, txn_date DESC, id DESC
            """
        ).fetchall()


def update_transaction_record(
    transaction_id: int,
    new_category: str,
    new_description: str,
    new_amount: Optional[float] = None,
) -> None:
    with get_db() as conn:
        if new_amount is None:
            conn.execute(
                """
                UPDATE transactions
                SET raw_description = ?,
                    clean_description = ?,
                    category = ?,
                    review_flag = 0
                WHERE id = ?
                """,
                (
                    new_description,
                    clean_description(new_description),
                    new_category,
                    transaction_id,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE transactions
                SET raw_description = ?,
                    clean_description = ?,
                    amount = ?,
                    category = ?,
                    review_flag = 0
                WHERE id = ?
                """,
                (
                    new_description,
                    clean_description(new_description),
                    new_amount,
                    new_category,
                    transaction_id,
                ),
            )

        conn.commit()


def mark_transaction_for_review(transaction_id: int) -> None:
    with get_db() as conn:
        conn.execute(
            """
            UPDATE transactions
            SET review_flag = 1
            WHERE id = ?
            """,
            (transaction_id,),
        )
        conn.commit()


def summarize_transactions(rows: List[sqlite3.Row]) -> Dict[str, Any]:
    if not rows:
        return {
            "total_count": 0,
            "total_income": 0.0,
            "total_expense": 0.0,
            "review_count": 0,
            "by_category": [],
        }

    df = pd.DataFrame([dict(r) for r in rows])

    total_income = float(df.loc[df["amount"] > 0, "amount"].sum())
    total_expense = float(abs(df.loc[df["amount"] < 0, "amount"].sum()))
    review_count = int(df["review_flag"].sum())

    category_rows = []

    grouped = df.groupby("category", dropna=False)
    for category, group in grouped:
        income_total = float(group.loc[group["amount"] > 0, "amount"].sum())
        expense_total = float(abs(group.loc[group["amount"] < 0, "amount"].sum()))
        net_total = float(group["amount"].sum())
        count_total = int(len(group))

        category_rows.append(
            {
                "category": category,
                "count": count_total,
                "income_total": income_total,
                "expense_total": expense_total,
                "net_total": net_total,
            }
        )

    category_rows = sorted(category_rows, key=lambda x: x["expense_total"], reverse=True)

    return {
        "total_count": len(rows),
        "total_income": total_income,
        "total_expense": total_expense,
        "review_count": review_count,
        "by_category": category_rows,
    }


def generate_narrative(rows: List[sqlite3.Row]) -> str:
    if not rows:
        return "No transactions have been uploaded yet."

    df = pd.DataFrame([dict(r) for r in rows])

    income = float(df.loc[df["amount"] > 0, "amount"].sum())
    expenses = float(abs(df.loc[df["amount"] < 0, "amount"].sum()))
    review_count = int(df["review_flag"].sum())

    expense_df = df[df["amount"] < 0].copy()
    if expense_df.empty:
        top_text = "No major expense categories were identified."
    else:
        top_categories = (
            expense_df.groupby("category")["amount"]
            .sum()
            .abs()
            .sort_values(ascending=False)
            .head(3)
        )
        top_text = ", ".join([f"{cat} (${amt:,.2f})" for cat, amt in top_categories.items()])

    return (
        f"This dataset shows total income of ${income:,.2f} and total expenses of ${expenses:,.2f}. "
        f"The largest spending categories were {top_text}. "
        f"There are {review_count} transaction(s) flagged for manual review."
    )


def extract_csv_transactions(file_path: Path) -> List[Dict[str, Any]]:
    df = pd.read_csv(file_path)

    date_col = next((c for c in df.columns if c.lower() in ["date", "transaction_date", "posted_date"]), None)
    desc_col = next((c for c in df.columns if c.lower() in ["description", "details", "memo"]), None)
    amount_col = next((c for c in df.columns if c.lower() in ["amount", "transaction_amount"]), None)

    if not date_col or not desc_col or not amount_col:
        raise ValueError("CSV must contain columns like date, description, and amount.")

    rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        txn_date = parse_date(str(row[date_col])) if pd.notna(row[date_col]) else None
        raw_description = str(row[desc_col]).strip() if pd.notna(row[desc_col]) else ""
        amount = None

        if pd.notna(row[amount_col]):
            try:
                amount = float(row[amount_col])
            except ValueError:
                amount = normalize_amount(str(row[amount_col]))

        if amount is None:
            amount = 0.0

        category = categorize_transaction(raw_description, amount)
        confidence = compute_confidence(raw_description, amount, txn_date, category, "csv")
        review_flag = needs_review(txn_date, raw_description, amount, category, confidence)

        rows.append(
            {
                "txn_date": txn_date,
                "raw_description": raw_description,
                "amount": amount,
                "category": category,
                "parse_method": "csv",
                "confidence": confidence,
                "review_flag": review_flag,
            }
        )

    return rows


def extract_text_from_pdf(file_path: Path) -> str:
    all_text: List[str] = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text()
            if page_text:
                all_text.append(f"\n--- PAGE {i} ---\n{page_text}")
    return "\n".join(all_text).strip()


def merge_broken_lines(lines: List[str]) -> List[str]:
    merged: List[str] = []
    i = 0

    while i < len(lines):
        current = lines[i].strip()

        if not current:
            i += 1
            continue

        starts_with_date = any(re.match(rf"^{pattern}\b", current) for pattern in DATE_PATTERNS)

        if starts_with_date and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            current_money_count = len(re.findall(AMOUNT_PATTERN, current))
            next_starts_with_date = any(re.match(rf"^{pattern}\b", next_line) for pattern in DATE_PATTERNS)

            if current_money_count == 0 and next_line and not next_starts_with_date:
                merged.append(current + " " + next_line)
                i += 2
                continue

        merged.append(current)
        i += 1

    return merged


def looks_like_transaction_line(line: str) -> bool:
    if is_probable_header_or_noise(line):
        return False

    line = line.strip()
    has_date = any(re.search(rf"^{pattern}\b", line) for pattern in DATE_PATTERNS)
    money_matches = re.findall(AMOUNT_PATTERN, line)

    if not has_date:
        return False
    if len(money_matches) < 1:
        return False
    return True


def normalize_outgoing_incoming_sign(description: str, amount: float) -> float:
    desc = clean_description(description)

    outgoing_words = [
        "pos", "purchase", "debit", "withdrawal", "payment", "atm", "fee",
        "check", "card purchase", "recurring"
    ]
    incoming_words = [
        "deposit", "payroll", "credit", "refund", "interest", "payment received"
    ]

    if any(word in desc for word in outgoing_words) and amount > 0:
        return -amount
    if any(word in desc for word in incoming_words) and amount < 0:
        return abs(amount)
    return amount


def parse_bank_transaction_line(line: str, source_file: str) -> Optional[Dict[str, Any]]:
    line = re.sub(r"\s+", " ", line).strip()

    date_match = re.match(r"^(" + "|".join(DATE_PATTERNS) + r")\s+(.*)$", line)
    if not date_match:
        return None

    date_text = date_match.group(1)
    rest = date_match.group(2).strip()

    amounts = re.findall(AMOUNT_PATTERN, rest)
    if not amounts:
        return None

    parsed_date = parse_date(date_text)
    parsed_amount = None

    if len(amounts) >= 2:
        parsed_amount = normalize_amount(amounts[-2])
        trailing_amounts_to_remove = amounts[-2:]
    else:
        parsed_amount = normalize_amount(amounts[-1])
        trailing_amounts_to_remove = amounts[-1:]

    description = rest
    for amt in reversed(trailing_amounts_to_remove):
        idx = description.rfind(amt)
        if idx != -1:
            description = description[:idx].rstrip()

    description = re.sub(r"\s+", " ", description).strip()

    if not description or is_probable_header_or_noise(description):
        return None

    if parsed_amount is None:
        return None

    parsed_amount = normalize_outgoing_incoming_sign(description, parsed_amount)
    category = categorize_transaction(description, parsed_amount)
    confidence = compute_confidence(description, parsed_amount, parsed_date, category, "pdf-bank")
    review_flag = needs_review(parsed_date, description, parsed_amount, category, confidence)

    return {
        "txn_date": parsed_date,
        "raw_description": description,
        "amount": parsed_amount,
        "category": category,
        "parse_method": "pdf-bank",
        "confidence": confidence,
        "review_flag": review_flag,
        "source_file": source_file,
    }


def extract_pdf_transactions_bank_first(file_path: Path) -> List[Dict[str, Any]]:
    full_text = extract_text_from_pdf(file_path)
    if not full_text.strip():
        return []

    bank_name = detect_bank(full_text)
    print(f"Detected bank: {bank_name}")

    raw_lines = [line.strip() for line in full_text.splitlines() if line.strip()]
    clean_lines = merge_broken_lines(raw_lines)

    rows: List[Dict[str, Any]] = []
    for line in clean_lines:
        if looks_like_transaction_line(line):
            parsed = parse_bank_transaction_line(line, file_path.name)
            if parsed:
                rows.append(parsed)

    return dedupe_extracted_rows(rows)


def parse_transaction_lines(lines: List[str], method: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    pending_date: Optional[str] = None
    pending_desc_parts: List[str] = []

    for line in lines:
        if is_probable_header_or_noise(line):
            continue

        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue

        date_match = re.search(r"\b(\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2}/\d{1,2}|\d{4}-\d{2}-\d{2})\b", line)
        amount_matches = re.findall(AMOUNT_PATTERN, line)

        if date_match and amount_matches:
            date_text = date_match.group(1)
            amount_text = amount_matches[-1]

            parsed_date = parse_date(date_text)
            parsed_amount = normalize_amount(amount_text)

            description = line.replace(date_text, "", 1)
            description = re.sub(re.escape(amount_text) + r"\s*$", "", description).strip()
            description = re.sub(r"\s+", " ", description)

            if description and not is_probable_header_or_noise(description):
                parsed_amount = 0.0 if parsed_amount is None else normalize_outgoing_incoming_sign(description, parsed_amount)
                category = categorize_transaction(description, parsed_amount)
                confidence = compute_confidence(description, parsed_amount, parsed_date, category, method)
                review_flag = needs_review(parsed_date, description, parsed_amount, category, confidence)

                rows.append(
                    {
                        "txn_date": parsed_date,
                        "raw_description": description,
                        "amount": parsed_amount,
                        "category": category,
                        "parse_method": method,
                        "confidence": confidence,
                        "review_flag": review_flag,
                    }
                )

            pending_date = None
            pending_desc_parts = []
            continue

        if date_match and not amount_matches:
            pending_date = date_match.group(1)
            remaining = line.replace(pending_date, "", 1).strip()
            pending_desc_parts = [remaining] if remaining else []
            continue

        if pending_date and amount_matches:
            amount_text = amount_matches[-1]
            parsed_date = parse_date(pending_date)
            parsed_amount = normalize_amount(amount_text)

            desc_line = re.sub(re.escape(amount_text) + r"\s*$", "", line).strip()
            if desc_line:
                pending_desc_parts.append(desc_line)

            description = " ".join(part for part in pending_desc_parts if part).strip()
            description = re.sub(r"\s+", " ", description)

            if description and not is_probable_header_or_noise(description):
                parsed_amount = 0.0 if parsed_amount is None else normalize_outgoing_incoming_sign(description, parsed_amount)
                category = categorize_transaction(description, parsed_amount)
                confidence = compute_confidence(description, parsed_amount, parsed_date, category, method)
                review_flag = needs_review(parsed_date, description, parsed_amount, category, confidence)

                rows.append(
                    {
                        "txn_date": parsed_date,
                        "raw_description": description,
                        "amount": parsed_amount,
                        "category": category,
                        "parse_method": method,
                        "confidence": confidence,
                        "review_flag": review_flag,
                    }
                )

            pending_date = None
            pending_desc_parts = []
            continue

        if pending_date and not amount_matches:
            pending_desc_parts.append(line)

    return rows


def extract_pdf_transactions_from_tables(file_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()

            for table in tables:
                if not table:
                    continue

                for raw_row in table:
                    if not raw_row:
                        continue

                    cleaned_cells = [str(cell).strip() if cell is not None else "" for cell in raw_row]
                    non_empty = [c for c in cleaned_cells if c]

                    if len(non_empty) < 2:
                        continue

                    date_candidate = None
                    amount_candidates: List[str] = []

                    for cell in cleaned_cells:
                        if not date_candidate and looks_like_date(cell):
                            date_candidate = cell
                        if looks_like_amount(cell):
                            amount_candidates.append(cell)

                    if not date_candidate or not amount_candidates:
                        continue

                    amount_candidate = amount_candidates[-1]
                    parsed_date = parse_date(date_candidate)
                    parsed_amount = normalize_amount(amount_candidate)

                    desc_parts = []
                    for cell in cleaned_cells:
                        if not cell:
                            continue
                        if cell == date_candidate:
                            continue
                        if cell == amount_candidate:
                            continue
                        if looks_like_amount(cell):
                            continue
                        desc_parts.append(cell)

                    description = " ".join([p for p in desc_parts if p]).strip()

                    if not description or is_probable_header_or_noise(description):
                        continue

                    parsed_amount = 0.0 if parsed_amount is None else normalize_outgoing_incoming_sign(description, parsed_amount)
                    category = categorize_transaction(description, parsed_amount)
                    confidence = compute_confidence(description, parsed_amount, parsed_date, category, "pdf-table")
                    review_flag = needs_review(parsed_date, description, parsed_amount, category, confidence)

                    rows.append(
                        {
                            "txn_date": parsed_date,
                            "raw_description": description,
                            "amount": parsed_amount,
                            "category": category,
                            "parse_method": "pdf-table",
                            "confidence": confidence,
                            "review_flag": review_flag,
                        }
                    )

    return rows


def extract_pdf_transactions_from_text(file_path: Path) -> List[Dict[str, Any]]:
    lines: List[str] = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            page_lines = [line.strip() for line in text.splitlines() if line.strip()]
            lines.extend(page_lines)

    return parse_transaction_lines(lines, "pdf-text")


def pdf_to_pil_images(file_path: Path) -> List[Image.Image]:
    images: List[Image.Image] = []
    doc = fitz.open(file_path)

    for page in doc:
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        images.append(img)

    doc.close()
    return images


def extract_pdf_transactions_from_ocr(file_path: Path) -> List[Dict[str, Any]]:
    if not EASYOCR_AVAILABLE or ocr_reader is None:
        return []

    lines: List[str] = []
    images = pdf_to_pil_images(file_path)

    for image in images:
        image_array = np.array(image).copy()
        results = ocr_reader.readtext(image_array, detail=0, paragraph=False)
        page_lines = [str(line).strip() for line in results if str(line).strip()]
        lines.extend(page_lines)

    return parse_transaction_lines(lines, "pdf-ocr")


def dedupe_extracted_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []

    for row in rows:
        key = (
            row.get("txn_date"),
            clean_description(row.get("raw_description", "")),
            round(float(row.get("amount", 0.0)), 2),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def extract_pdf_transactions(file_path: Path) -> List[Dict[str, Any]]:
    bank_rows = extract_pdf_transactions_bank_first(file_path)
    if bank_rows:
        print(f"Bank-first PDF rows found: {len(bank_rows)}")
        return bank_rows

    table_rows = dedupe_extracted_rows(extract_pdf_transactions_from_tables(file_path))
    text_rows = dedupe_extracted_rows(extract_pdf_transactions_from_text(file_path))

    print(f"PDF table rows found: {len(table_rows)}")
    print(f"PDF text rows found: {len(text_rows)}")

    best_rows = table_rows if len(table_rows) >= len(text_rows) else text_rows
    if best_rows:
        return best_rows

    print("Trying OCR fallback...")
    ocr_rows = dedupe_extracted_rows(extract_pdf_transactions_from_ocr(file_path))
    print(f"PDF OCR rows found: {len(ocr_rows)}")
    return ocr_rows


def export_transactions_csv(rows: List[sqlite3.Row]) -> Path:
    export_path = EXPORTS_DIR / "categorized_transactions_export.csv"

    with export_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "txn_date",
                "raw_description",
                "clean_description",
                "amount",
                "category",
                "parse_method",
                "confidence",
                "review_flag",
                "source_file",
                "created_at",
            ]
        )

        for row in rows:
            writer.writerow(
                [
                    row["txn_date"],
                    row["raw_description"],
                    row["clean_description"],
                    row["amount"],
                    row["category"],
                    row["parse_method"],
                    row["confidence"],
                    row["review_flag"],
                    row["source_file"],
                    row["created_at"],
                ]
            )

    return export_path


@app.route("/", methods=["GET"])
def index():
    transactions = load_all_transactions()
    summary = summarize_transactions(transactions)
    narrative = generate_narrative(transactions)
    return render_template(
        "index.html",
        transactions=transactions,
        summary=summary,
        narrative=narrative,
        categories=ALL_CATEGORIES,
    )


@app.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("statement_file")

    if not files or all(file.filename == "" for file in files):
        flash("Please choose up to 12 CSV or PDF files.")
        return redirect(url_for("index"))

    if len(files) > 12:
        flash("You can upload up to 12 files at a time.")
        return redirect(url_for("index"))

    total_processed = 0
    processed_files = 0

    for file in files:
        if not file or file.filename == "":
            continue

        filename = Path(file.filename).name
        suffix = Path(filename).suffix.lower()

        if suffix not in [".csv", ".pdf"]:
            flash(f"Skipped {filename}: only CSV and PDF files are supported.")
            continue

        save_path = UPLOADS_DIR / filename
        file.save(save_path)

        try:
            if suffix == ".csv":
                rows = extract_csv_transactions(save_path)
            else:
                rows = extract_pdf_transactions(save_path)

            if not rows:
                flash(f"No transactions extracted from {filename}.")
                continue

            inserted_count = save_transactions(rows, filename)
            total_processed += inserted_count
            processed_files += 1

            if inserted_count == 0:
                flash(f"{filename} was processed, but no new transactions were added (possible duplicate upload).")

        except Exception as exc:
            flash(f"Error processing {filename}: {exc}")

    if processed_files == 0:
        flash("No files were successfully processed.")
    else:
        flash(f"Processed {processed_files} file(s) and saved {total_processed} new transaction(s).")

    return redirect(url_for("index"))


@app.route("/update_transaction/<int:transaction_id>", methods=["POST"])
def update_transaction(transaction_id: int):
    new_category = request.form.get("category", "").strip()
    new_description = request.form.get("description", "").strip()
    amount_text = request.form.get("amount", "").strip()

    if not new_category:
        return jsonify({"success": False, "message": "Please choose a category."}), 400

    if not new_description:
        return jsonify({"success": False, "message": "Description cannot be empty."}), 400

    new_amount = None
    if amount_text:
        try:
            new_amount = float(amount_text)
        except ValueError:
            return jsonify({"success": False, "message": "Amount must be a valid number."}), 400

    update_transaction_record(
        transaction_id=transaction_id,
        new_category=new_category,
        new_description=new_description,
        new_amount=new_amount,
    )

    return jsonify(
        {
            "success": True,
            "message": "Transaction updated successfully.",
            "transaction_id": transaction_id,
            "category": new_category,
            "description": new_description,
            "amount": new_amount,
        }
    )


@app.route("/mark_for_review/<int:transaction_id>", methods=["POST"])
def mark_for_review(transaction_id: int):
    mark_transaction_for_review(transaction_id)

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(
            {
                "success": True,
                "message": "Transaction marked for review.",
                "transaction_id": transaction_id,
            }
        )

    flash("Transaction marked for review.")
    return redirect(url_for("index"))


@app.route("/export", methods=["GET"])
def export_csv():
    transactions = load_all_transactions()

    if not transactions:
        flash("No transactions available to export.")
        return redirect(url_for("index"))

    export_path = export_transactions_csv(transactions)
    return send_file(export_path, as_attachment=True)


@app.route("/reset", methods=["POST"])
def reset_data():
    with get_db() as conn:
        conn.execute("DELETE FROM transactions")
        conn.commit()

    flash("All transaction data has been cleared.")
    return redirect(url_for("index"))


if __name__ == "__main__":
    init_db()
    app.run(debug=True)