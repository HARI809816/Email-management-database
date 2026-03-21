import re
from typing import Any
from fastapi import APIRouter
from datetime import datetime
from pydantic import BaseModel

from database import get_db, get_next_serial_batch
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


class ExternalAppWrapper(BaseModel):
    """
    Wrapper object sent by the external app.
    Records are read from 'data' if present, otherwise from 'preview'.
    """
    status:  str                  = "success"
    summary: Any                  = None
    preview: list[IngestRecord]   = []
    data:    list[IngestRecord]   = []


@router.post("", response_model=IngestResponse)
async def ingest_records(payload: ExternalAppWrapper):
    """
    Receive records from another FastAPI service.
    
    Optimized for bulk handling:
      1. Batch check existing emails in ONE query.
      2. Fetch a range of serial numbers in ONE query.
      3. Insert all new records in ONE query.
    """
    # Use 'data' if it has entries, otherwise fall back to 'preview'
    records = payload.data if payload.data else payload.preview
    if not records:
        return IngestResponse(inserted=0, skipped=0, message="No records provided.")

    db = get_db()
    skipped_details: list[IngestSkipped] = []
    
    # ── 1. Initial Filtering & Format Validation (In-memory) ─────────────────
    to_check_in_db = []
    seen_in_batch  = set()
    
    for record in records:
        raw_email   = str(record.email).strip().lower() if record.email   else ""
        raw_name    = record.name.strip()               if record.name    else ""
        raw_country = record.country.strip()            if record.country else ""

        # Validate required fields
        missing = []
        if not raw_name:    missing.append("name")
        if not raw_email:   missing.append("email")
        if not raw_country: missing.append("country")

        if missing:
            skipped_details.append(IngestSkipped(
                email=raw_email or None, name=raw_name or None,
                reason=f"Missing required field(s): {', '.join(missing)}"
            ))
            continue

        # Validate format
        if not _EMAIL_RE.match(raw_email):
            skipped_details.append(IngestSkipped(
                email=raw_email, name=raw_name, 
                reason=f"Invalid email format: '{raw_email}'"
            ))
            continue

        # In-batch duplicate check
        if raw_email in seen_in_batch:
            skipped_details.append(IngestSkipped(
                email=raw_email, name=raw_name, 
                reason="Duplicate within the incoming batch list"
            ))
            continue
            
        seen_in_batch.add(raw_email)
        to_check_in_db.append((record, raw_email, raw_name, raw_country))

    if not to_check_in_db:
        return IngestResponse(
            inserted=0, skipped=len(skipped_details),
            message="No valid records to process.",
            skipped_details=skipped_details
        )

    # ── 2. Batch Existence Check (ONE DB Call) ───────────────────────────────
    batch_emails   = [x[1] for x in to_check_in_db]
    cursor         = db["raw"].find({"email": {"$in": batch_emails}}, {"email": 1})
    db_existing    = {doc["email"] async for doc in cursor}

    # ── 3. Final List Preparation ────────────────────────────────────────────
    ready_to_insert = []
    for record, email, name, country in to_check_in_db:
        if email in db_existing:
            skipped_details.append(IngestSkipped(
                email=email, name=name, 
                reason="Duplicate email — already exists in the database"
            ))
            continue
        ready_to_insert.append((record, email, name, country))

    if not ready_to_insert:
        return IngestResponse(
            inserted=0, skipped=len(skipped_details),
            message="All new records were duplicates of existing DB entries.",
            skipped_details=skipped_details
        )

    # ── 4. Batch Serial Allocation (ONE DB Call) ─────────────────────────────
    start_serial = await get_next_serial_batch(len(ready_to_insert))

    # ── 5. Build Documents ───────────────────────────────────────────────────
    docs = []
    for i, (record, email, name, country) in enumerate(ready_to_insert):
        domain = (record.domain or "").strip() or _derive_domain(email)
        docs.append({
            "serial_no":  start_serial + i,
            "name":       name,
            "email":      email,
            "country":    country,
            "date_added": datetime.utcnow(),
            "domain":            domain or None,
            "phone_number":      (record.phone_number or "").strip() or None,
            "label":             (record.label or "").strip()        or None,
            "status":            (record.status or "").strip()       or None,
            "mail_sender_name":  (record.mail_sender_name or "").strip() or None,
            "profile_name":      (record.profile_name or "").strip() or None,
            "mail_sending_date": record.mail_sending_date or None,
            "validation":         False,
        })


    # ── 6. Bulk Insert (ONE DB Call) ─────────────────────────────────────────
    inserted_count = 0
    try:
        await db["raw"].insert_many(docs, ordered=False)
        inserted_count = len(docs)
    except Exception as exc:
        # Some might have failed (e.g. race condition unique index)
        # Motor BulkWriteError has details.nInserted
        inserted_count = getattr(exc, "details", {}).get("nInserted", 0)
        skipped_details.append(IngestSkipped(
            reason=f"Bulk insert partial failure/error: {exc}"
        ))

    # ── 7. History & Response ────────────────────────────────────────────────
    skipped_count = len(skipped_details)
    status        = "success" if skipped_count == 0 else "partial"
    notes         = f"{skipped_count} skipped." if skipped_count else None

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
