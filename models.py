from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime


# ─────────────────────────────────────────────
#  INGEST
# ─────────────────────────────────────────────

class IngestRecord(BaseModel):
    """Schema for incoming records from external FastAPI.
    
    Required : name, email, country
    Optional : domain, phone_number, label, status,
               mail_sender_name, profile_name, mail_sending_date
    """
    # ── Required ──────────────────────────────
    name:             str
    email:            str   # kept as str; validated manually in ingest route
    country:          str

    # ── Optional ──────────────────────────────
    domain:           Optional[str]      = None
    phone_number:     Optional[str]      = None
    label:            Optional[str]      = None
    status:           Optional[str]      = None
    mail_sender_name: Optional[str]      = None
    profile_name:     Optional[str]      = None
    mail_sending_date:Optional[datetime] = None


class IngestSkipped(BaseModel):
    """Details about a record that was skipped during ingest."""
    email:  Optional[str] = None
    name:   Optional[str] = None
    reason: str


class IngestResponse(BaseModel):
    inserted:        int
    skipped:         int
    message:         str
    skipped_details: List[IngestSkipped] = []


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
    serial_no:         int
    name:              str
    email:             str
    country:           str
    date_added:        datetime
    # optional extended fields
    domain:            Optional[str]      = None
    phone_number:      Optional[str]      = None
    label:             Optional[str]      = None
    status:            Optional[str]      = None
    mail_sender_name:  Optional[str]      = None
    profile_name:      Optional[str]      = None
    mail_sending_date: Optional[datetime] = None


# ─────────────────────────────────────────────
#  VALIDATED RECORD (response shape)
# ─────────────────────────────────────────────

class ValidatedRecord(BaseModel):
    serial_no:         int
    name:              str
    email:             str
    country:           str
    date_added:        datetime
    validated_at:      datetime
    # optional extended fields
    domain:            Optional[str]      = None
    phone_number:      Optional[str]      = None
    label:             Optional[str]      = None
    status:            Optional[str]      = None
    mail_sender_name:  Optional[str]      = None
    profile_name:      Optional[str]      = None
    mail_sending_date: Optional[datetime] = None


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
