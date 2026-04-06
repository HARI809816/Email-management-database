"""
ingest_client.py
────────────────
Drop this file into your external app.

Usage:
    from ingest_client import ingest

    records = [
        {"name": "Alice", "email": "alice@example.com", "country": "US"},
        ...
    ]
    result = ingest(records)
    print(result)  # {"inserted": ..., "skipped": ..., "message": ...}

That's it. No need to think about blob vs direct — it's handled automatically.
"""

import json
import os
import httpx

# ── Config: set these in your .env / environment ──────────────────────────────
API_BASE         = os.getenv("API_BASE",         "https://email-management-database.vercel.app")
BLOB_READ_WRITE_TOKEN = os.getenv("BLOB_READ_WRITE_TOKEN", "")

# If total records exceed this, blob upload is used automatically
BLOB_THRESHOLD   = 5_000   # safe limit well under Vercel's ~4.5 MB body cap


def ingest(records: list[dict]) -> dict:
    """
    Send records to the Data Manager API.

    • ≤ 5,000 records → POST /ingest  (direct JSON, fast)
    • > 5,000 records → uploads to Vercel Blob first, then POST /ingest/blob
                        (transparent — you don't need to do anything extra)

    Returns the API response dict:
        { "inserted": int, "skipped": int, "message": str, "skipped_details": [...] }
    """
    if not records:
        return {"inserted": 0, "skipped": 0, "message": "No records provided."}

    if len(records) <= BLOB_THRESHOLD:
        return _ingest_direct(records)
    else:
        return _ingest_via_blob(records)


# -----------------------------------------------------------------------------
#  Internal helpers — you don't need to call these directly
# -----------------------------------------------------------------------------

def _ingest_direct(records: list[dict]) -> dict:
    """POST records as JSON directly (for small batches)."""
    print(f"[ingest] Direct JSON -> {len(records)} records")
    with httpx.Client(timeout=120) as client:
        r = client.post(
            f"{API_BASE}/ingest",
            json={"data": records},
        )
        r.raise_for_status()
        return r.json()


def _ingest_via_blob(records: list[dict]) -> dict:
    """
    Upload records as a JSON blob, then trigger ingest from the URL.
    Used automatically when records > BLOB_THRESHOLD.
    """
    print(f"[ingest] Large payload ({len(records)} records) -> using Vercel Blob")

    if not BLOB_READ_WRITE_TOKEN:
        raise EnvironmentError(
            "BLOB_READ_WRITE_TOKEN is not set. "
            "Add it to your .env file."
        )

    # ── 1. Upload JSON to Vercel Blob ─────────────────────────────────────────
    body = json.dumps({"data": records}).encode("utf-8")

    with httpx.Client(timeout=180) as client:
        upload = client.put(
            "https://blob.vercel-storage.com/emails-upload.json",
            content=body,
            headers={
                "Authorization": f"Bearer {BLOB_READ_WRITE_TOKEN}",
                "x-content-type": "application/json",
                "x-access": "public",
            },
        )
        upload.raise_for_status()
        blob_url = upload.json()["url"]
        print(f"[ingest] Blob URL: {blob_url}")

    # ── 2. Tell the API to ingest from that URL ───────────────────────────────
    with httpx.Client(timeout=600) as client:
        r = client.post(
            f"{API_BASE}/ingest/blob",
            json={"blob_url": blob_url, "delete_after": True},
        )
        r.raise_for_status()
        result = r.json()

    print(f"[ingest] Done -> inserted: {result.get('inserted')}, skipped: {result.get('skipped')}")
    return result
