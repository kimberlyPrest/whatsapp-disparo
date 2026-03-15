"""
Rotas de carregamento de contatos (planilha e CSV).
"""

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from auth import get_current_user
from contacts.parsers import load_from_sheet, parse_csv_contacts
from database import _db_get

router = APIRouter()


@router.post("/api/load")
def load_contacts_route(body: dict, user_id: str = Depends(get_current_user)):
    sheet_url = body.get("sheet_url", "")
    if not sheet_url:
        raise HTTPException(400, "sheet_url obrigatório.")
    rows = _db_get("user_credentials", filters={"user_id": user_id}, columns="google_credentials")
    if not rows or not rows[0].get("google_credentials"):
        raise HTTPException(400, "Configure as credenciais Google primeiro em Configuração.")
    google_creds = rows[0]["google_credentials"]
    try:
        contacts = load_from_sheet(sheet_url, google_creds)
        return {"contacts": contacts}
    except Exception as e:
        raise HTTPException(400, str(e))


@router.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...), _: str = Depends(get_current_user)):
    content = await file.read()
    try:
        contacts = parse_csv_contacts(content)
        return {"contacts": contacts, "filename": file.filename}
    except Exception as e:
        raise HTTPException(400, f"Erro ao processar CSV: {e}")
