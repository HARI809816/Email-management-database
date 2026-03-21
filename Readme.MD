# Data Manager API
A high-performance FastAPI + MongoDB project to ingest, validate, and manage email records with bulk-optimized processing and full history logging.

---

## 🔥 Key Features

- **Bulk Optimized Ingest**: Handles thousands of records in seconds using batch duplicate checking and bulk insertion.
- **Automated Validation Proxy**: Fetches raw data, proxies it to an external validation app, and saves successful results.
- **Admin Reset API**: Destructively clear `raw` or `validated` tables and reset serial ID counters via Swagger UI.
- **History Logging**: Automatically tracks every action (ingest, validate, download) with success/failure status and record counts.

---

## 🏗️ Project Structure

```
project/
├── main.py                  # App entry point & router registration
├── database.py              # MongoDB connection + bulk serial counter
├── models.py                # Pydantic schemas (IngestRecord, ValidatedRecord, etc.)
├── utils.py                 # Shared helpers (query builder, history logger, serializer)
├── routes/
│   ├── ingest.py            # POST /ingest (Bulk-optimized)
│   ├── raw.py               # POST /raw/view, POST /raw/download (CSV)
│   ├── validate.py          # POST /validate (Automated Proxy to external app)
│   ├── validated.py         # POST /validated/view, POST /validated/download (CSV)
│   ├── history.py           # GET /history (Activity logs)
│   └── admin.py             # POST /admin/reset (Clear tables & Reset serials)
├── .env                     # MongoDB URI, DB name, and External Validation URL
└── requirements.txt         # Python dependencies (fastapi, motor, httpx, etc.)
```

---

## 🚀 Installation & Setup

1. **Clone & Setup Environment**
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   pip install -r requirements.txt
   ```

2. **Configure `.env`**
   ```text
   MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
   DB_NAME=EmailDataBase
   EXTERNAL_VALIDATION_URL=http://your-app-url/validate
   ```

3. **Run Server**
   ```bash
   uvicorn main:app --reload
   ```
   - **Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
   - **Health Check**: [http://localhost:8000/](http://localhost:8000/)

---

## 📊 Database Schema

### `raw` Collection
Stores all incoming records. Duplicates are filtered in memory before hitting DB.

| Required Field | Optional Field |
|:---|:---|
| `name` | `domain` (Auto-derived from email if absent) |
| `email` (Unique) | `phone_number`, `label`, `status` |
| `country` | `mail_sender_name`, `profile_name`, `mail_sending_date` |

### `validated` Collection
Stores records successfully returned by the external validation service.
- **Includes**: All raw fields + `validated_at` timestamp.

### `history` Collection
Logs every system action: `ingest`, `validate`, `download_raw`, `download_validated`.

---

## ⚙️ Bulk Ingest Optimization
The `/ingest` endpoint is designed for high-volume uploads:
- **Batch Lookups**: Uses `$in` to check thousands of existing emails in ONE query.
- **Bulk Insert**: Uses `insert_many` to save all new records in ONE query.
- **Serial Batching**: Allocates blocks of serial IDs atomically to prevent race conditions.

---

## 🔒 Admin Actions
Use the `/admin/reset` endpoint in Swagger to:
1. Delete all records from `raw` or `validated`.
2. Reset the ID counter back to **1**.
*Note: History is preserved for auditing unless explicitly cleared.*

---

## 📡 External Validation Workflow
1. **Pull**: Fetches raw records based on `FilterParams` (date range, serial range, limit).
2. **Proxy**: POSTs raw JSON to `EXTERNAL_VALIDATION_URL`.
3. **Wait**: External app validates and returns a list of "Good" records.
4. **Save**: Successful records are automatically stored in the `validated` collection.