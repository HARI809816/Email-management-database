import json
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from datetime import datetime
from database import get_db
from models import (
    ValidateFilterParams, ValidateResponse, ValidatedRecord, ValidatedIngest
)
from utils import build_validate_query, log_history, serialize_doc

router = APIRouter(prefix="/validate", tags=["Validate"])


@router.post("")
async def export_for_validation(filters: ValidateFilterParams = ValidateFilterParams()):
    """
    Stage 1: Export records for validation.
    Optimized for ALL dataset sizes:
    1. Uses a cursor to avoid loading everything into memory.
    2. Streams results as NDJSON.
    3. Updates records in small batches (1,000) WHILE streaming to ensure only
       exported records are marked, and to reduce initial latency.
    """
    db    = get_db()
    query = build_validate_query(filters)
    query["validation"] = {"$ne": True}

    # Step 1: Pre-check if ANY records exist matching the query
    # (High performance check before starting the stream)
    exists = await db["raw"].find_one(query, {"_id": 1})
    if not exists:
        return {"status": "success", "message": "No records found matching the given filters.", "records": []}

    # Step 2: Initialize Cursor
    cursor = db["raw"].find(query, {"_id": 0}).sort("serial_no", 1)
    
    if filters.limit:
        cursor = cursor.limit(filters.limit)

    async def record_generator():
        current_batch_emails = []
        record_count = 0
        
        async for doc in cursor:
            # Yield record immediately for low latency
            yield json.dumps(serialize_doc(doc)) + "\n"
            
            # Add to batch for validation update
            if "email" in doc:
                current_batch_emails.append(doc["email"])
                record_count += 1
            
            # Update in chunks of 500
            if len(current_batch_emails) >= 500:
                await db["raw"].update_many(
                    {"email": {"$in": current_batch_emails}},
                    {"$set": {"validation": True}}
                )
                current_batch_emails = []

        # Final batch update
        if current_batch_emails:
            await db["raw"].update_many(
                {"email": {"$in": current_batch_emails}},
                {"$set": {"validation": True}}
            )
            
        # Log History silently at the end (could be in a background task)
        # Note: log_history is async and we are inside a generator
        # It's better to log after the stream, but StreamingResponse takes over.
        # However, it works fine in FastAPI/Motor.
        await log_history(
            action="export_for_validation",
            record_count=record_count,
            status="success",
            filters=filters,
            notes=f"Exported and locked {record_count} records for processing."
        )

    return StreamingResponse(
        record_generator(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": "attachment; filename=export_validation.jsonl"}
    )


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
        email   = (r.email   or "").strip().lower()
        name    = (r.name    or "").strip()
        country = (r.country or "").strip()

        # 1. Mandatory field check
        if not email or not name or not country:
            skipped_count += 1
            continue
            
        # 2. Duplicate check (DB + Batch)
        if email in db_existing or email in seen_in_batch:
            skipped_count += 1
            continue
            
        seen_in_batch.add(email)
        
        # Prepare the doc matching the full schema
        doc = r.dict()
        doc["validated_at"] = now
        # Ensure validated/cleaned values are used
        doc["email"]   = email
        doc["name"]    = name
        doc["country"] = country
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

