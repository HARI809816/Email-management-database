from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from datetime import datetime


# ─────────────────────────────────────────────
#  INGEST
# ─────────────────────────────────────────────

class IngestRecord(BaseModel):
    """Schema for incoming records from external FastAPI."""
    name: str
    email: EmailStr
    country: str


class IngestResponse(BaseModel):
    inserted: int
    skipped: int
    message: str


# ─────────────────────────────────────────────
#  SHARED FILTER
# ─────────────────────────────────────────────

class FilterParams(BaseModel):
    """All filters are optional — apply only what's provided."""
    date_from:    Optional[datetime] = None   # e.g. "2024-01-01T00:00:00"
    date_to:      Optional[datetime] = None
    serial_from:  Optional[int]      = None   # serial_no range start
    serial_to:    Optional[int]      = None   # serial_no range end
    limit:        Optional[int]      = None   # max records (None = all)
    offset:       Optional[int]      = 0      # skip N records


# ─────────────────────────────────────────────
#  RAW RECORD (response shape)
# ─────────────────────────────────────────────

class RawRecord(BaseModel):
    serial_no:  int
    name:       str
    email:      str
    country:    str
    date_added: datetime


# ─────────────────────────────────────────────
#  VALIDATED RECORD (response shape)
# ─────────────────────────────────────────────

class ValidatedRecord(BaseModel):
    serial_no:    int
    name:         str
    email:        str
    country:      str
    date_added:   datetime
    validated_at: datetime


# ─────────────────────────────────────────────
#  VALIDATE ACTION RESPONSE
# ─────────────────────────────────────────────

class ValidateResponse(BaseModel):
    validated: int
    skipped:   int
    message:   str
    records:   list[ValidatedRecord] = []


# ─────────────────────────────────────────────
#  HISTORY RECORD (response shape)
# ─────────────────────────────────────────────

class HistoryRecord(BaseModel):
    action:       str        # ingest / validate / download_raw / download_validated
    performed_at: datetime
    record_count: int
    filters_used: Optional[dict] = None
    status:       str        # success / partial / failed
    notes:        Optional[str] = None
