"""
Parsing e normalização de contatos (CSV, Google Sheets).
"""

import csv
import io

import gspread
from google.oauth2.service_account import Credentials

from config import COL_NAME, COL_PHONE


def normalize_phone(phone: str) -> str:
    digits = "".join(filter(str.isdigit, str(phone)))
    if not digits.startswith("55"):
        digits = "55" + digits
    return digits


def _parse_contacts(records: list[dict]) -> list[dict]:
    contacts = []
    for row in records:
        name = str(row.get(COL_NAME, "")).strip()
        phone = str(row.get(COL_PHONE, "")).strip()
        if name and phone:
            contacts.append(
                {
                    "name": name,
                    "first_name": name.split()[0].capitalize(),
                    "phone": normalize_phone(phone),
                }
            )
    return contacts


def load_from_sheet(sheet_url: str, google_creds: dict) -> list[dict]:
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
    client = gspread.authorize(creds)
    raw = client.open_by_url(sheet_url).sheet1.get_all_records()
    records = [{k.strip(): v for k, v in row.items()} for row in raw]
    return _parse_contacts(records)


def parse_csv_contacts(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    records = [{k.strip(): v for k, v in row.items()} for row in reader]
    return _parse_contacts(records)
