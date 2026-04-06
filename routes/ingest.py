import re
import os
import httpx
from typing import Any
from fastapi import APIRouter, HTTPException
from datetime import datetime
from pydantic import BaseModel
from pymongo.errors import BulkWriteError

from database import get_db, get_next_serial_batch
from models import IngestRecord, IngestResponse, IngestSkipped
from utils import log_history

# Basic email format check — must have exactly one @, a dot in the domain part
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

router = APIRouter(prefix="/ingest", tags=["Ingest"])

# ── Chunking constants ────────────────────────────────────────────────────────
AUTO_SPLIT_THRESHOLD = 8_000   # if payload > this, split into chunks
CHUNK_SIZE           = 5_000   # records per chunk
DB_IN_CHUNK          = 2_000   # max emails per MongoDB $in query
MAX_SKIP_DETAILS     = 200     # max skipped records to return in response


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


async def _batch_exists(collection, emails: list[str]) -> set[str]:
    """
    Check which emails already exist in the DB.
    Splits into DB_IN_CHUNK-sized $in queries to stay well under the 16 MB BSON limit.
    """
    found: set[str] = set()
    for i in range(0, len(emails), DB_IN_CHUNK):
        chunk = emails[i : i + DB_IN_CHUNK]
        async for doc in collection.find({"email": {"$in": chunk}}, {"email": 1}):
            found.add(doc["email"])
    return found


async def _process_chunk(
    db,
    records: list[IngestRecord],
    seen_globally: set[str],
) -> tuple[int, list[IngestSkipped]]:
    """
    Process one chunk of records end-to-end.
    Returns (inserted_count, skipped_details).
    `seen_globally` is updated in-place to track emails across chunks.
    """
    skipped: list[IngestSkipped] = []
    to_check_in_db = []

    # ── 1. In-memory filtering ────────────────────────────────────────────────
    for record in records:
        raw_email   = str(record.email).strip().lower() if record.email   else ""
        raw_name    = record.name.strip()               if record.name    else ""
        raw_country = record.country.strip()            if record.country else ""

        missing = []
        if not raw_name:    missing.append("name")
        if not raw_email:   missing.append("email")
        if not raw_country: missing.append("country")

        if missing:
            skipped.append(IngestSkipped(
                email=raw_email or None, name=raw_name or None,
                reason=f"Missing required field(s): {', '.join(missing)}"
            ))
            continue

        if not _EMAIL_RE.match(raw_email):
            skipped.append(IngestSkipped(
                email=raw_email, name=raw_name,
                reason=f"Invalid email format: '{raw_email}'"
            ))
            continue

        # Cross-chunk + within-chunk duplicate guard
        if raw_email in seen_globally:
            skipped.append(IngestSkipped(
                email=raw_email, name=raw_name,
                reason="Duplicate within the incoming batch list"
            ))
            continue

        seen_globally.add(raw_email)
        to_check_in_db.append((record, raw_email, raw_name, raw_country))

    if not to_check_in_db:
        return 0, skipped

    # ── 2. Batch DB existence check (chunked $in) ─────────────────────────────
    batch_emails = [x[1] for x in to_check_in_db]
    db_existing  = await _batch_exists(db["raw"], batch_emails)

    # ── 3. Final list preparation ─────────────────────────────────────────────
    ready_to_insert = []
    for record, email, name, country in to_check_in_db:
        if email in db_existing:
            skipped.append(IngestSkipped(
                email=email, name=name,
                reason="Duplicate email — already exists in the database"
            ))
        else:
            ready_to_insert.append((record, email, name, country))

    if not ready_to_insert:
        return 0, skipped

    # ── 4. Batch serial allocation ────────────────────────────────────────────
    start_serial = await get_next_serial_batch(len(ready_to_insert))

    # ── 5. Build documents ────────────────────────────────────────────────────
    docs = []
    for i, (record, email, name, country) in enumerate(ready_to_insert):
        domain = (record.domain or "").strip() or _derive_domain(email)
        docs.append({
            "serial_no":         start_serial + i,
            "name":              name,
            "email":             email,
            "country":           country,
            "date_added":        datetime.utcnow(),
            "domain":            domain or None,
            "phone_number":      (record.phone_number or "").strip()      or None,
            "label":             (record.label or "").strip()             or None,
            "status":            (record.status or "").strip()            or None,
            "mail_sender_name":  (record.mail_sender_name or "").strip()  or None,
            "profile_name":      (record.profile_name or "").strip()      or None,
            "mail_sending_date": record.mail_sending_date                 or None,
            "validation":        False,
        })

    # ── 6. Bulk insert ────────────────────────────────────────────────────────
    inserted_count = 0
    try:
        await db["raw"].insert_many(docs, ordered=False)
        inserted_count = len(docs)
    except BulkWriteError as bwe:
        inserted_count = bwe.details.get("nInserted", 0)
        skipped.append(IngestSkipped(
            reason=f"Bulk insert partial failure: {bwe.details.get('writeErrors', '')}"
        ))
    except Exception as exc:
        inserted_count = getattr(exc, "details", {}).get("nInserted", 0)
        skipped.append(IngestSkipped(
            reason=f"Bulk insert error: {exc}"
        ))

    return inserted_count, skipped


@router.post("", response_model=IngestResponse)
async def ingest_records(payload: ExternalAppWrapper):
    """
    Receive records from another FastAPI service.

    Auto-chunking:
      - If total records <= 8,000  → processed in one pass.
      - If total records  > 8,000  → automatically split into 5,000-record
        chunks and processed sequentially. This avoids MongoDB BSON limits,
        socket timeouts, and large response bodies.
    """
    records = payload.data if payload.data else payload.preview
    if not records:
        return IngestResponse(inserted=0, skipped=0, message="No records provided.")

    db = get_db()
    total_inserted  = 0
    all_skipped:  list[IngestSkipped] = []
    seen_globally: set[str] = set()   # dedup across all chunks

    # ── Auto-split into chunks if payload is large ────────────────────────────
    if len(records) > AUTO_SPLIT_THRESHOLD:
        chunks = [records[i : i + CHUNK_SIZE] for i in range(0, len(records), CHUNK_SIZE)]
    else:
        chunks = [records]

    for chunk in chunks:
        inserted, skipped = await _process_chunk(db, chunk, seen_globally)
        total_inserted += inserted
        all_skipped.extend(skipped)

    # ── Cap skipped_details in response to avoid huge payloads ────────────────
    skipped_count = len(all_skipped)
    truncated     = skipped_count > MAX_SKIP_DETAILS
    response_skip = all_skipped[:MAX_SKIP_DETAILS]

    message = f"{total_inserted} inserted, {skipped_count} skipped."
    if truncated:
        message += f" (showing first {MAX_SKIP_DETAILS} skip reasons)"

    # ── History log ───────────────────────────────────────────────────────────
    status = "success" if skipped_count == 0 else "partial"
    notes  = f"{skipped_count} skipped." if skipped_count else None
    await log_history(
        action="ingest",
        record_count=total_inserted,
        status=status,
        notes=notes
    )

    return IngestResponse(
        inserted=total_inserted,
        skipped=skipped_count,
        message=message,
        skipped_details=response_skip,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Vercel Blob Ingest
# ─────────────────────────────────────────────────────────────────────────────

class BlobIngestRequest(BaseModel):
    """
    Request body for blob-based ingestion.
    The external app uploads a JSON file to Vercel Blob, then sends
    only the URL here — keeping the HTTP body tiny (~100 bytes).
    """
    blob_url:     str  # Public URL returned by Vercel Blob after upload
    delete_after: bool = True  # Auto-delete blob from storage after ingest


@router.post("/blob", response_model=IngestResponse)
async def ingest_from_blob(payload: BlobIngestRequest):
    """
    Download a JSON file from Vercel Blob and ingest its records.

    Supports any file size (Vercel Blob allows up to 5 TB).
    Completely bypasses Vercel's 4.5 MB serverless body limit.

    JSON format accepted (both work):
      • Wrapper: { "data": [ {...}, ... ] }   ← same as POST /ingest
      • Bare:    [ {...}, ... ]
    """
    # ── 1. Download JSON from Vercel Blob ─────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            response = await client.get(payload.blob_url)
            response.raise_for_status()
            raw = response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch blob: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Blob download error: {e}")

    # ── 2. Parse records (support wrapper dict or bare list) ──────────────────
    try:
        if isinstance(raw, list):
            # Bare array: [{"name":..., "email":..., "country":...}, ...]
            records = [IngestRecord(**item) for item in raw]
        elif isinstance(raw, dict):
            # Wrapper: {"data": [...]} or {"preview": [...]}
            wrapper  = ExternalAppWrapper(**raw)
            records  = wrapper.data if wrapper.data else wrapper.preview
        else:
            raise ValueError("Unexpected JSON structure")
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"JSON parse error: {e}")

    if not records:
        return IngestResponse(inserted=0, skipped=0, message="No records in blob file.")

    # ── 3. Auto-chunk + insert (reuses existing logic) ────────────────────────
    db            = get_db()
    total_inserted = 0
    all_skipped:   list[IngestSkipped] = []
    seen_globally: set[str]            = set()

    chunks = (
        [records[i : i + CHUNK_SIZE] for i in range(0, len(records), CHUNK_SIZE)]
        if len(records) > AUTO_SPLIT_THRESHOLD
        else [records]
    )

    for chunk in chunks:
        ins, skipped = await _process_chunk(db, chunk, seen_globally)
        total_inserted += ins
        all_skipped.extend(skipped)

    # ── 4. Optionally delete blob from storage ────────────────────────────────
    if payload.delete_after:
        token = os.getenv("BLOB_READ_WRITE_TOKEN", "")
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                await client.delete(
                    payload.blob_url,
                    headers={"Authorization": f"Bearer {token}"},
                )
        except Exception:
            pass  # Non-fatal — log silently, don't fail the response

    # ── 5. Cap skipped_details + build response ───────────────────────────────
    skipped_count = len(all_skipped)
    truncated     = skipped_count > MAX_SKIP_DETAILS
    response_skip = all_skipped[:MAX_SKIP_DETAILS]

    message = f"{total_inserted} inserted, {skipped_count} skipped."
    if truncated:
        message += f" (showing first {MAX_SKIP_DETAILS} skip reasons)"

    status = "success" if skipped_count == 0 else "partial"
    notes  = f"{skipped_count} skipped." if skipped_count else None
    await log_history(
        action="ingest_blob",
        record_count=total_inserted,
        status=status,
        notes=notes,
    )

    return IngestResponse(
        inserted=total_inserted,
        skipped=skipped_count,
        message=message,
        skipped_details=response_skip,
    )
