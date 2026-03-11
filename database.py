from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os
import certifi

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME", "data_manager")

client: AsyncIOMotorClient = None
db = None


async def connect_db():
    """Initialize MongoDB connection and create indexes."""
    global client, db

    # tlsAllowInvalidCertificates=True fixes the TLSV1_ALERT_INTERNAL_ERROR
    # that occurs on Python 3.11 + Windows when connecting to MongoDB Atlas.
    client = AsyncIOMotorClient(
        MONGO_URI,
        tls=True,
        tlsCAFile=certifi.where(),
        tlsAllowInvalidCertificates=True
    )
    db = client[DB_NAME]

    # ── Unique index on email in raw & validated ──────────────────────────────
    await db["raw"].create_index("email", unique=True)
    await db["validated"].create_index("email", unique=True)

    # ── Index on serial_no for range queries ──────────────────────────────────
    await db["raw"].create_index("serial_no")
    await db["validated"].create_index("serial_no")

    # ── Index on date_added for date range queries ────────────────────────────
    await db["raw"].create_index("date_added")
    await db["validated"].create_index("date_added")

    # ── Ensure counters doc exists ────────────────────────────────────────────
    await db["counters"].update_one(
        {"_id": "raw_serial"},
        {"$setOnInsert": {"seq": 0}},
        upsert=True
    )
    print("MongoDB connected and indexes created.")


async def close_db():
    """Close MongoDB connection."""
    global client
    if client:
        client.close()
        print("MongoDB connection closed.")


async def get_next_serial() -> int:
    """
    Atomically increment and return the next serial number.
    Uses MongoDB findAndModify (findOneAndUpdate) for safe concurrency.
    """
    result = await db["counters"].find_one_and_update(
        {"_id": "raw_serial"},
        {"$inc": {"seq": 1}},
        return_document=True,
        upsert=True
    )
    return result["seq"]


def get_db():
    """Return the active database instance."""
    return db