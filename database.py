from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os
import certifi

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "data_manager")

_client: AsyncIOMotorClient = None
_db = None


def _get_client():
    """Return (and lazily create) the MongoDB client."""
    global _client, _db
    if _client is None:
        _client = AsyncIOMotorClient(
            MONGO_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            # Required on Vercel (Python 3.12 + OpenSSL 3 vs MongoDB Atlas TLS)
            tlsAllowInvalidCertificates=True,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=20000,
        )
        _db = _client[DB_NAME]
    return _client, _db


async def connect_db():
    """
    Called by FastAPI lifespan. Initialises the client and creates indexes.
    All errors are caught so a transient Atlas issue never kills the process.
    """
    global _client, _db
    _client, _db = _get_client()

    try:
        db = _db
        await db["raw"].create_index("email", unique=True)
        await db["validated"].create_index("email", unique=True)
        await db["raw"].create_index("serial_no")
        await db["validated"].create_index("serial_no")
        await db["raw"].create_index("date_added")
        await db["validated"].create_index("date_added")
        await db["counters"].update_one(
            {"_id": "raw_serial"},
            {"$setOnInsert": {"seq": 0}},
            upsert=True,
        )
        print("MongoDB connected and indexes ensured.")
    except Exception as exc:
        # Log but do NOT raise – a startup crash kills the whole serverless
        # function. Routes will return a 503 if the DB is truly unreachable.
        print(f"[WARNING] MongoDB startup error (will retry on first request): {exc}")


async def close_db():
    """Close MongoDB connection (no-op in serverless – processes are ephemeral)."""
    global _client
    if _client:
        _client.close()
        _client = None
        print("MongoDB connection closed.")


async def get_next_serial() -> int:
    """Atomically increment and return the next serial number."""
    _, db = _get_client()
    result = await db["counters"].find_one_and_update(
        {"_id": "raw_serial"},
        {"$inc": {"seq": 1}},
        return_document=True,
        upsert=True,
    )
    return result["seq"]


def get_db():
    """Return the active database instance (lazy-connect if needed)."""
    _, db = _get_client()
    return db