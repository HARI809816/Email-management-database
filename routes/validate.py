import httpx
import os
from fastapi import APIRouter, HTTPException
from datetime import datetime

from database import get_db
from models import FilterParams, ValidateResponse, ValidatedRecord
from utils import build_query, log_history, serialize_doc

router = APIRouter(prefix="/validate", tags=["Validate"])

EXTERNAL_VALIDATION_URL = os.getenv("EXTERNAL_VALIDATION_URL")

@router.post("", response_model=ValidateResponse)
async def validate_records(filters: FilterParams = FilterParams()):
    """
    1. Fetch raw records matching filters.
    2. POST them to External User App for validation.
    3. Save the returned 'Good' records into validated collection.
    """
    db    = get_db()
    query = build_query(filters)

    # Step 1: Fetch matching raw records
    cursor = db["raw"].find(query, {"_id": 0}).sort("serial_no", 1)
    if filters.offset:
        cursor = cursor.skip(filters.offset)
    if filters.limit:
        cursor = cursor.limit(filters.limit)

    raw_records = await cursor.to_list(length=None)

    if not raw_records:
        return ValidateResponse(
            validated=0,
            skipped=0,
            message="No records found matching the given filters."
        )

    # Serialize records to handle datetime objects before sending JSON
    raw_records = [serialize_doc(doc) for doc in raw_records]

    # Step 2 & 3: Send to External App and wait for response
    if not EXTERNAL_VALIDATION_URL:
        raise HTTPException(status_code=500, detail="EXTERNAL_VALIDATION_URL not configured in .env")

    try:
        async with httpx.AsyncClient() as client:
            # We send the raw records to the external app
            response = await client.post(
                EXTERNAL_VALIDATION_URL, 
                json=raw_records,
                timeout=60.0 # Validation might take time
            )
            response.raise_for_status()
            validated_from_external = response.json() # Expected list of validated records
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"External Validation App Error: {str(e)}")

    if not isinstance(validated_from_external, list):
        raise HTTPException(status_code=502, detail="External App returned invalid format (expected list)")

    # Step 4: Save returned JSON into validated table
    validated_count = 0
    skipped_count   = 0
    now             = datetime.utcnow()
    inserted_records = []

    for record in validated_from_external:
        # Deduplication check
        email = record.get("email")
        if not email:
            skipped_count += 1
            continue

        existing = await db["validated"].find_one({"email": email})
        if existing:
            skipped_count += 1
            continue

        # Add validation timestamp
        record["validated_at"] = now
        
        try:
            await db["validated"].insert_one(record)
            # Remove the _id before adding to response if MongoDB added it
            if "_id" in record:
                del record["_id"]
            inserted_records.append(record)
            validated_count += 1
        except Exception:
            skipped_count += 1

    # Log results
    status = "success" if skipped_count == 0 else "partial"
    await log_history(
        action="validate",
        record_count=validated_count,
        status=status,
        filters=filters,
        notes=f"Externally validated. {skipped_count} skipped."
    )

    return ValidateResponse(
        validated=validated_count,
        skipped=skipped_count,
        message=f"{validated_count} validated via external service, {skipped_count} skipped.",
        records=inserted_records
    )
