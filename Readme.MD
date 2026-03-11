# Data Manager API
A FastAPI + MongoDB project to ingest, validate, and manage records with full history logging.

---

## Project Structure

```
project/
├── main.py                  # App entry point
├── database.py              # MongoDB connection + serial counter
├── models.py                # Pydantic schemas
├── utils.py                 # Shared helpers (query builder, history logger)
├── routes/
│   ├── ingest.py            # POST /ingest
│   ├── raw.py               # POST /raw/view, POST /raw/download
│   ├── validate.py          # POST /validate
│   ├── validated.py         # POST /validated/view, POST /validated/download
│   └── history.py           # GET /history
├── .env                     # MongoDB URI and DB name
└── requirements.txt         # Python dependencies
```

---

## Requirements

- Python 3.10+
- MongoDB running locally or via Atlas

---

## Installation

```bash
# 1. Clone or download the project
cd project

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure your .env file
# Edit .env and set your MONGO_URI and DB_NAME

# 4. Run the server
uvicorn main:app --reload
```

API will be live at: http://localhost:8000
Swagger docs at:     http://localhost:8000/docs

---

## Collections (Tables)

### raw
Stores all incoming records from the external FastAPI service.

| Field      | Type     | Notes                        |
|------------|----------|------------------------------|
| serial_no  | int      | Auto-incremented             |
| name       | string   |                              |
| email      | string   | Unique — duplicates skipped  |
| country    | string   |                              |
| date_added | datetime | Auto-set by server           |

### validated
Stores records that have been reviewed and moved from raw.

| Field        | Type     | Notes                        |
|--------------|----------|------------------------------|
| serial_no    | int      | Carried from raw             |
| name         | string   |                              |
| email        | string   | Unique — duplicates skipped  |
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
| POST   | /ingest  | Receive records from external FastAPI |

Request body (list):
```json
[
  { "name": "Alice", "email": "alice@example.com", "country": "USA" }
]
```

---

### Raw
| Method | Endpoint      | Description         |
|--------|---------------|---------------------|
| POST   | /raw/view     | View raw records    |
| POST   | /raw/download | Download raw as CSV |

---

### Validate
| Method | Endpoint  | Description                         |
|--------|-----------|-------------------------------------|
| POST   | /validate | Move raw records to validated table |

---

### Validated
| Method | Endpoint            | Description               |
|--------|---------------------|---------------------------|
| POST   | /validated/view     | View validated records    |
| POST   | /validated/download | Download validated as CSV |

---

### History
| Method | Endpoint | Description                            |
|--------|----------|----------------------------------------|
| GET    | /history | View all logs (filter by action param) |

Example: `GET /history?action=validate&limit=20&offset=0`

---

## Filter Parameters

All view, download, and validate endpoints accept these optional filters in the request body:

```json
{
  "date_from":   "2024-01-01T00:00:00",
  "date_to":     "2024-12-31T23:59:59",
  "serial_from": 1,
  "serial_to":   100,
  "limit":       50,
  "offset":      0
}
```

All fields are optional. If none are provided, all records are returned.

---

## Environment Variables (.env)

```
MONGO_URI=mongodb://localhost:27017
DB_NAME=data_manager
```

---

## Notes

- Duplicate emails are automatically skipped during ingest and validation
- serial_no is safely auto-incremented using MongoDB atomic findAndModify
- Every action (ingest, validate, download) is automatically logged to history
- Visit /docs for the full interactive Swagger UI to test all endpoints