import csv
import io
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from database import get_db
from models import FilterParams
from utils import build_query, log_history

router = APIRouter(prefix="/validated", tags=["Validated"])


async def fetch_validated_records(filters: FilterParams) -> list[dict]:
    """Shared helper to query validated collection with filters."""
    db    = get_db()
    query = build_query(filters)

    cursor = db["validated"].find(query, {"_id": 0}).sort("serial_no", 1)

    if filters.offset:
        cursor = cursor.skip(filters.offset)
    if filters.limit:
        cursor = cursor.limit(filters.limit)

    return await cursor.to_list(length=None)


# ── View ──────────────────────────────────────────────────────────────────────

@router.post("/view")
async def view_validated(filters: FilterParams = FilterParams()):
    """
    View validated records with optional filters.
    """
    records = await fetch_validated_records(filters)
    return {
        "total":   len(records),
        "records": records
    }


# ── Download ──────────────────────────────────────────────────────────────────

@router.post("/download")
async def download_validated(filters: FilterParams = FilterParams()):
    """
    Download validated records as a CSV file with optional filters.
    """
    records = await fetch_validated_records(filters)

    if not records:
        return {"message": "No validated records found for the given filters."}

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
    output.seek(0)

    await log_history(
        action="download_validated",
        record_count=len(records),
        filters=filters,
        notes=f"Downloaded {len(records)} validated records."
    )

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=validated_data.csv"}
    )
