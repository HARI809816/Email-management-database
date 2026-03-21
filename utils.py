from datetime import datetime
from models import FilterParams
from database import get_db


def build_query(filters: FilterParams) -> dict:
    """
    Build a MongoDB query dict from optional filter parameters.
    Only adds conditions for fields that are actually provided.
    """
    query = {}

    # ── Date range ────────────────────────────────────────────────────────────
    if filters.date_from or filters.date_to:
        query["date_added"] = {}
        if filters.date_from:
            query["date_added"]["$gte"] = filters.date_from
        if filters.date_to:
            query["date_added"]["$lte"] = filters.date_to

    # ── Serial number range ───────────────────────────────────────────────────
    if filters.serial_from is not None or filters.serial_to is not None:
        query["serial_no"] = {}
        if filters.serial_from is not None:
            query["serial_no"]["$gte"] = filters.serial_from
        if filters.serial_to is not None:
            query["serial_no"]["$lte"] = filters.serial_to

    # ── Domain ───────────────────────────────────────────────────────────────
    if filters.domain:
        query["domain"] = filters.domain.strip()

    # ── Country ──────────────────────────────────────────────────────────────
    if filters.country:
        query["country"] = filters.country.strip()

    return query


async def log_history(
    action: str,
    record_count: int,
    status: str = "success",
    filters: FilterParams = None,
    notes: str = None
):
    """Insert a history log entry into the history collection."""
    db = get_db()
    log = {
        "action":       action,
        "performed_at": datetime.utcnow(),
        "record_count": record_count,
        "filters_used": filters.model_dump(exclude_none=True) if filters else None,
        "status":       status,
        "notes":        notes,
    }
    await db["history"].insert_one(log)


def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document to JSON-serializable dict (handling _id and datetime)."""
    if "_id" in doc:
        doc["id"] = str(doc.pop("_id"))
    
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
            
    return doc
