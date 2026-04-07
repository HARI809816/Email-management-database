from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Any
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
    # ── Fields (all optional to prevent 422 blocking) ─────
    name:             Optional[str]      = None
    email:            Optional[str]      = None
    country:          Optional[str]      = None

    # ── Optional ──────────────────────────────
    domain:           Optional[str]      = None
    email_domain:     Optional[str]      = None   # sent by external app; same as domain
    phone_number:     Optional[str]      = None
    label:            Optional[str]      = None
    status:           Optional[str]      = None
    mail_sender_name: Optional[str]      = None
    profile_name:     Optional[str]      = None
    mail_sending_date:Optional[Any]      = None
    validation:        bool              = False


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
    domain:       Optional[str]      = None   # e.g. "gmail.com"
    country:      Optional[str]      = None   # e.g. "USA"
    limit:        Optional[int]      = None   # max records (None = all)
    offset:       Optional[int]      = 0      # skip N records


class ValidateFilterParams(BaseModel):
    """Simplified filters specifically for exporting to validation."""
    from_date:      Optional[datetime] = None
    to_date:        Optional[datetime] = None
    limit:          Optional[int]      = None
    serial_no_from: Optional[int]      = None
    sno_to:         Optional[int]      = None
    country:        Optional[str]      = None
    domain:         Optional[str]      = None



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
    validation:         bool              = False


# ─────────────────────────────────────────────
#  VALIDATED RECORD (response shape)
# ─────────────────────────────────────────────

class ValidatedRecord(BaseModel):
    serial_no:         int
    name:              Optional[str]      = None
    email:             Optional[str]      = None
    country:           Optional[str]      = None
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


class ValidatedIngest(BaseModel):
    """Schema for bulk validated records uploaded from frontend."""
    records: list[ValidatedRecord]


class BulkValidationUpdate(BaseModel):
    """Request schema for bulk updating the validation status in the raw collection."""
    emails:   Optional[list[str]] = None
    filters:  Optional[ValidateFilterParams] = None
    validation: bool



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
