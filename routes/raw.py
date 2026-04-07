import csv
import io
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from database import get_db
from models import FilterParams, RawRecord, BulkValidationUpdate
from utils import build_query, build_validate_query, log_history, serialize_doc

router = APIRouter(prefix="/raw", tags=["Raw"])


@router.post("/bulk-validation")
async def bulk_validation_update(payload: BulkValidationUpdate):
    """
    Bulk update the validation status of records in the raw table.
    Can update by a list of emails OR by filters.
    """
    db = get_db()
    query = {}
    
    if payload.emails:
        query["email"] = {"$in": payload.emails}
    elif payload.filters:
        query = build_validate_query(payload.filters)
    else:
        return {"message": "Either emails or filters must be provided.", "updated": 0}

    result = await db["raw"].update_many(
        query,
        {"$set": {"validation": payload.validation}}
    )

    await log_history(
        action="bulk_validation_update",
        record_count=result.modified_count,
        status="success",
        notes=f"Bulk updated {result.modified_count} records to validation={payload.validation}."
    )

    return {
        "message": f"Successfully updated {result.modified_count} records.",
        "matched": result.matched_count,
        "modified": result.modified_count
    }


async def fetch_raw_records(filters: FilterParams) -> list[dict]:
    """Shared helper to query raw collection with filters."""
    db    = get_db()
    query = build_query(filters)

    cursor = db["raw"].find(query, {"_id": 0}).sort("serial_no", 1)

    if filters.offset:
        cursor = cursor.skip(filters.offset)
    if filters.limit:
        cursor = cursor.limit(filters.limit)

    return await cursor.to_list(length=None)


# ── View ──────────────────────────────────────────────────────────────────────

@router.post("/view")
async def view_raw(filters: FilterParams = FilterParams()):
    """
    View raw records with optional filters.
    All filter fields are optional.
    """
    records = await fetch_raw_records(filters)
    return {
        "total":   len(records),
        "records": records
    }


# ── Download ──────────────────────────────────────────────────────────────────

@router.post("/download")
async def download_raw(filters: FilterParams = FilterParams()):
    """
    Download raw records as a CSV file with optional filters.
    """
    records = await fetch_raw_records(filters)

    if not records:
        return {"message": "No records found for the given filters."}

    # Build CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
    output.seek(0)

    await log_history(
        action="download_raw",
        record_count=len(records),
        filters=filters,
        notes=f"Downloaded {len(records)} raw records."
    )

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=raw_data.csv"}
    )
