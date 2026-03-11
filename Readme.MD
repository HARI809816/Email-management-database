# Data Manager API
A FastAPI + MongoDB project to ingest, validate, and manage records with full history logging.

---

## Project Structure

```
project/
├── main.py                  # App entry point
├── database.py              # MongoDB connection + serial counter
├── models.py                # Pydantic schemas
├── utils.py                 # Shared helpers (query builder, history logger, serializer)
├── routes/
│   ├── ingest.py            # POST /ingest
│   ├── raw.py               # POST /raw/view, POST /raw/download
│   ├── validate.py          # POST /validate (Automated Proxy)
│   ├── validated.py         # POST /validated/view, POST /validated/download
│   └── history.py           # GET /history
├── .env                     # MongoDB URI, DB name, and External Validation URL
└── requirements.txt         # Python dependencies
```

---

## Requirements

- Python 3.11+
- MongoDB (Local or Atlas)
- httpx (for external validation)

---

## Installation

```bash
# 1. Clone or download the project
cd project

# 2. Create and activate virtual environment
python -m venv venv
.\venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure your .env file
# Edit .env and set your MONGO_URI, DB_NAME, and EXTERNAL_VALIDATION_URL

# 5. Run the server
uvicorn main:app --reload
```

API will be live at: http://localhost:8000
Swagger docs at:     http://localhost:8000/docs

---

## External Validation Workflow

This API supports an **Automated Validation Proxy** workflow:
1. **Pull**: The server fetches raw records based on your filters (date, serial number, etc.).
2. **Proxy**: The server POSTs these records to your `EXTERNAL_VALIDATION_URL`.
3. **Wait**: The server waits for your external application to process the emails and return a JSON list of "Good" records.
4. **Save**: The server automatically saves the returned records to the `validated` collection.

---

## Collections (Tables)

### raw
Stores all incoming records.

| Field      | Type     | Notes                        |
|------------|----------|------------------------------|
| serial_no  | int      | Auto-incremented             |
| name       | string   |                              |
| email      | string   | Unique - duplicates skipped  |
| country    | string   |                              |
| date_added | datetime | Auto-set by server           |

### validated
Stores records returned by the external validation service.

| Field        | Type     | Notes                        |
|--------------|----------|------------------------------|
| serial_no    | int      | Carried from raw             |
| name         | string   |                              |
| email        | string   | Unique - duplicates skipped  |
| country      | string   |                              |
| date_added   | datetime | Carried from raw             |
| validated_at | datetime | Auto-set at validation time  |

### history
Logs every action performed in the system.

| Field        | Type     | Notes                                                    |
|--------------|----------|----------------------------------------------------------|
| action       | string   | ingest / validate / download_raw / download_validated    |
| performed_at | datetime | Auto-set by server                                       |
| record_count | int      | Number of records affected                               |
| filters_used | object   | Snapshot of filters used                                 |
| status       | string   | success / partial / failed                               |
| notes        | string   | e.g. "3 duplicates skipped"                              |

---

## API Endpoints

### Ingest
| Method | Endpoint | Description                           |
|--------|----------|---------------------------------------|
| POST   | /ingest  | Receive records for the raw collection |

### Raw
| Method | Endpoint      | Description         |
|--------|---------------|---------------------|
| POST   | /raw/view     | View raw records    |
| POST   | /raw/download | Download raw as CSV |

### Validate (The Automated Proxy)
| Method | Endpoint  | Description                                      |
|--------|-----------|--------------------------------------------------|
| POST   | /validate | Trigger external validation and move to validated |

**Note**: This endpoint coordinates with the `EXTERNAL_VALIDATION_URL` defined in your `.env`.

### Validated
| Method | Endpoint            | Description               |
|--------|---------------------|---------------------------|
| POST   | /validated/view     | View validated records    |
| POST   | /validated/download | Download validated as CSV |

---

## Environment Variables (.env)

```text
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
DB_NAME=EmailDataBase
EXTERNAL_VALIDATION_URL=http://your-app-url/validate
```

---

## Technical Notes

- **Deduplication**: Duplicate emails are automatically skipped during both ingest and validation.
- **JSON Serialization**: The system handles MongoDB `datetime` objects by converting them to ISO strings before communicating with external APIs.
- **Stability**: Removed terminal emojis to ensure compatibility with Windows Command Prompt and PowerShell (prevents UnicodeEncodeError).
- **Atomic Counters**: `serial_no` is incremented using MongoDB's atomic `findAndModify` to ensure no overlaps.