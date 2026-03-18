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
            serverSelectionTimeoutMS=5000,
        )
        _db = _client[DB_NAME]
    return _client, _db


async def connect_db():
    """
    Called by the FastAPI lifespan on startup.
    In serverless environments (Vercel) this is a no-op – the client is
    created lazily on the first request so cold-starts don't time out.
    """
    global _client, _db
    if _client is None:
        _client, _db = _get_client()[0], _get_client()[1]

    # Create indexes once per process start (safe to call repeatedly).
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