import re
from fastapi import APIRouter, Request, HTTPException
from datetime import datetime

from database import get_db, get_next_serial
from models import IngestRecord, IngestResponse, IngestSkipped
from utils import log_history

# Basic email format check — must have exactly one @, a dot in the domain part
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

router = APIRouter(prefix="/ingest", tags=["Ingest"])


def _derive_domain(email: str) -> str:
    """Extract domain from email address (e.g. alice@example.com → example.com)."""
    try:
        return email.split("@", 1)[1].strip().lower()
    except Exception:
        return ""


@router.post("", response_model=IngestResponse)
async def ingest_records(request: Request):
    """
    Receive records from another FastAPI service.

    Accepts two formats:
      1. A plain list:  [{name, email, country, ...}, ...]
      2. A wrapper object from an external app:
            {
              "status": "...",
              "summary": {...},
              "preview": [{name, email, country, ...}, ...],
              "data":    [{name, email, country, ...}, ...]   ← used if present, else preview
            }

    Required fields  : name, email, country
    Optional fields  : domain (auto-derived if absent), phone_number, label,
                       status, mail_sender_name, profile_name, mail_sending_date
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body.")

    # ── Determine the list of raw dicts ──────────────────────────────────────
    if isinstance(body, list):
        # Format 1: plain list sent directly
        raw_list = body
    elif isinstance(body, dict):
        # Format 2: wrapper object — prefer "data", fall back to "preview"
        if "data" in body and isinstance(body["data"], list):
            raw_list = body["data"]
        elif "preview" in body and isinstance(body["preview"], list):
            raw_list = body["preview"]
        else:
            raise HTTPException(
                status_code=422,
                detail='Wrapper object must contain a "data" or "preview" list.'
            )
    else:
        raise HTTPException(status_code=422, detail="Body must be a list or a wrapper object.")

    # ── Parse each raw dict into IngestRecord ─────────────────────────────────
    try:
        records = [IngestRecord(**item) for item in raw_list]
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Record validation error: {exc}")

    # ── Process records ───────────────────────────────────────────────────────
    db = get_db()
    inserted_count  = 0
    skipped_count   = 0
    skipped_details: list[IngestSkipped] = []

    for record in records:
        raw_email   = str(record.email).strip()   if record.email   else ""
        raw_name    = record.name.strip()          if record.name    else ""
        raw_country = record.country.strip()       if record.country else ""

        # ── 1. Validate required fields ──────────────────────────────────────
        missing = []
        if not raw_name:
            missing.append("name")
        if not raw_email:
            missing.append("email")
        if not raw_country:
            missing.append("country")

        if missing:
            skipped_details.append(IngestSkipped(
                email=raw_email or None,
                name=raw_name  or None,
                reason=f"Missing required field(s): {', '.join(missing)}"
            ))
            skipped_count += 1
            continue

        # ── 2. Validate email format ─────────────────────────────────────────
        if not _EMAIL_RE.match(raw_email):
            skipped_details.append(IngestSkipped(
                email=raw_email,
                name=raw_name,
                reason=f"Invalid email format: '{raw_email}'"
            ))
            skipped_count += 1
            continue

        # ── 3. Duplicate check ───────────────────────────────────────────────
        existing = await db["raw"].find_one({"email": raw_email})
        if existing:
            skipped_details.append(IngestSkipped(
                email=raw_email,
                name=raw_name,
                reason="Duplicate email — already exists in the database"
            ))
            skipped_count += 1
            continue

        # ── 4. Auto-derive domain if not provided ────────────────────────────
        domain = (record.domain or "").strip() or _derive_domain(raw_email)

        # ── 5. Build document ─────────────────────────────────────────────────
        serial = await get_next_serial()
        doc = {
            "serial_no":  serial,
            "name":       raw_name,
            "email":      raw_email,
            "country":    raw_country,
            "date_added": datetime.utcnow(),
            # optional — stored as None if not provided
            "domain":            domain or None,
            "phone_number":      (record.phone_number or "").strip() or None,
            "label":             (record.label or "").strip()        or None,
            "status":            (record.status or "").strip()       or None,
            "mail_sender_name":  (record.mail_sender_name or "").strip() or None,
            "profile_name":      (record.profile_name or "").strip() or None,
            "mail_sending_date": record.mail_sending_date or None,
        }

        # ── 6. Insert ─────────────────────────────────────────────────────────
        try:
            await db["raw"].insert_one(doc)
            inserted_count += 1
        except Exception as exc:
            skipped_details.append(IngestSkipped(
                email=raw_email,
                name=raw_name,
                reason=f"DB insert error: {exc}"
            ))
            skipped_count += 1

    # ── History log ───────────────────────────────────────────────────────────
    status = "success" if skipped_count == 0 else "partial"
    notes  = f"{skipped_count} skipped." if skipped_count else None

    await log_history(
        action="ingest",
        record_count=inserted_count,
        status=status,
        notes=notes
    )

    return IngestResponse(
        inserted=inserted_count,
        skipped=skipped_count,
        message=f"{inserted_count} inserted, {skipped_count} skipped.",
        skipped_details=skipped_details
    )
