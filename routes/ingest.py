from fastapi import APIRouter
from datetime import datetime
from pymongo.errors import BulkWriteError

from database import get_db, get_next_serial
from models import IngestRecord, IngestResponse
from utils import log_history

router = APIRouter(prefix="/ingest", tags=["Ingest"])


@router.post("", response_model=IngestResponse)
async def ingest_records(records: list[IngestRecord]):
    """
    Receive a list of records from another FastAPI service.
    - Auto-assigns serial_no and date_added.
    - Skips duplicate emails silently.
    """
    db = get_db()
    inserted_count = 0
    skipped_count  = 0

    for record in records:
        # Check duplicate email
        existing = await db["raw"].find_one({"email": record.email})
        if existing:
            skipped_count += 1
            continue

        serial = await get_next_serial()
        doc = {
            "serial_no":  serial,
            "name":       record.name,
            "email":      record.email,
            "country":    record.country,
            "date_added": datetime.utcnow(),
        }

        try:
            await db["raw"].insert_one(doc)
            inserted_count += 1
        except Exception:
            skipped_count += 1

    status = "success" if skipped_count == 0 else "partial"
    notes  = f"{skipped_count} duplicate(s) skipped." if skipped_count else None

    await log_history(
        action="ingest",
        record_count=inserted_count,
        status=status,
        notes=notes
    )

    return IngestResponse(
        inserted=inserted_count,
        skipped=skipped_count,
        message=f"{inserted_count} inserted, {skipped_count} skipped."
    )
