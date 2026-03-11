from fastapi import APIRouter, Query
from typing import Optional

from database import get_db

router = APIRouter(prefix="/history", tags=["History"])


@router.get("")
async def get_history(
    action: Optional[str] = Query(
        default=None,
        description="Filter by action: ingest | validate | download_raw | download_validated"
    ),
    limit:  Optional[int] = Query(default=50,  description="Max records to return"),
    offset: Optional[int] = Query(default=0,   description="Skip N records"),
):
    """
    View history logs.
    Optionally filter by action type, with pagination support.
    """
    db    = get_db()
    query = {}

    if action:
        query["action"] = action

    cursor = (
        db["history"]
        .find(query, {"_id": 0})
        .sort("performed_at", -1)   # latest first
        .skip(offset)
        .limit(limit)
    )

    records = await cursor.to_list(length=None)

    # Convert datetime to string for JSON serialization
    for r in records:
        if "performed_at" in r and r["performed_at"]:
            r["performed_at"] = r["performed_at"].isoformat()
        if "filters_used" in r and r["filters_used"]:
            # Serialize any datetime inside filters_used
            for k, v in r["filters_used"].items():
                if hasattr(v, "isoformat"):
                    r["filters_used"][k] = v.isoformat()

    return {
        "total":   len(records),
        "records": records
    }
