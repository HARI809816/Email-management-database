from fastapi import APIRouter
from datetime import datetime
from database import get_db
from models import (
    ValidateFilterParams, ValidateResponse, ValidatedRecord, ValidatedIngest
)
from utils import build_validate_query, log_history, serialize_doc

router = APIRouter(prefix="/validate", tags=["Validate"])


@router.post("", response_model=list[dict])
async def export_for_validation(filters: ValidateFilterParams = ValidateFilterParams()):
    """
    Stage 1: Export records for validation.
    1. Fetch raw records matching simplified filters AND validation == False.
    2. Mark these records as validation = True (Auto-lock).
    3. Return them to the frontend for processing.
    """
    db    = get_db()
    query = build_validate_query(filters)
    # Only fetch records NOT yet exported/validated
    # Use $ne: True to include records where the field is False OR missing (None)
    query["validation"] = {"$ne": True}

    # Step 1: Fetch matching raw records
    cursor = db["raw"].find(query, {"_id": 0}).sort("serial_no", 1)
    
    if filters.limit:
        cursor = cursor.limit(filters.limit)

    raw_records = await cursor.to_list(length=None)

    if not raw_records:
        return []

    # Step 2: Mark as processing (Auto-lock)
    batch_emails = [doc["email"] for doc in raw_records]
    if batch_emails:
        await db["raw"].update_many(
            {"email": {"$in": batch_emails}},
            {"$set": {"validation": True}}
        )

    # Step 3: Log & Return
    await log_history(
        action="export_for_validation",
        record_count=len(raw_records),
        status="success",
        filters=filters,
        notes=f"Exported {len(raw_records)} records for processing."
    )

    return [serialize_doc(doc) for doc in raw_records]


@router.post("/bulk-insert", response_model=ValidateResponse)
async def bulk_insert_validated(payload: ValidatedIngest):
    """
    Stage 2: Bulk ingest validated records from frontend.
    Optimized for performance: 
      1. Batch check duplicates.
      2. Bulk insert new records.
    """
    records = payload.records
    if not records:
        return ValidateResponse(validated=0, skipped=0, message="No records provided.")

    db = get_db()
    skipped_count = 0
    now = datetime.utcnow()
    
    # ── 1. Batch Duplicate Check ─────────────────────────────────────────────
    incoming_emails = [r.email.strip().lower() for r in records if r.email]
    cursor          = db["validated"].find({"email": {"$in": incoming_emails}}, {"email": 1})
    db_existing     = {doc["email"] async for doc in cursor}

    # ── 2. Prepare Documents ────────────────────────────────────────────────
    ready_to_insert = []
    seen_in_batch   = set()

    for r in records:
        email = (r.email or "").strip().lower()
        if not email:
            skipped_count += 1
            continue
            
        if email in db_existing or email in seen_in_batch:
            skipped_count += 1
            continue
            
        seen_in_batch.add(email)
        
        # Prepare the doc matching the full schema
        doc = r.dict()
        doc["validated_at"] = now
        # Ensure email is consistent
        doc["email"] = email
        ready_to_insert.append(doc)

    # ── 3. Bulk Insert ───────────────────────────────────────────────────────
    inserted_count = 0
    if ready_to_insert:
        try:
            await db["validated"].insert_many(ready_to_insert, ordered=False)
            inserted_count = len(ready_to_insert)
        except Exception as exc:
            # Handle partial success (e.g. unique constraint race condition)
            inserted_count = getattr(exc, "details", {}).get("nInserted", 0)
            skipped_count += (len(ready_to_insert) - inserted_count)

    # ── 4. History & Response ────────────────────────────────────────────────
    status = "success" if skipped_count == 0 else "partial"
    await log_history(
        action="bulk_validate_ingest",
        record_count=inserted_count,
        status=status,
        notes=f"Bulk ingested validated records. {skipped_count} skipped."
    )

    # Prepare response records (limit for sanity if huge)
    response_records = [ValidatedRecord(**r) for r in ready_to_insert[:100]]

    return ValidateResponse(
        validated=inserted_count,
        skipped=skipped_count,
        message=f"{inserted_count} inserted, {skipped_count} skipped.",
        records=response_records
    )

