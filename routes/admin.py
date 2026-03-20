from fastapi import APIRouter
from enum import Enum
from database import get_db

router = APIRouter(prefix="/admin", tags=["Admin"])

class CollectionEnum(str, Enum):
    raw = "raw"
    validated = "validated"

@router.post("/reset")
async def reset_table(collection: CollectionEnum):
    """
    Clears the selected collection and resets its serial number counter to start from 1.
    
    Warning: This action is destructive and cannot be undone.
    """
    db = get_db()
    
    coll_name = collection.value
    
    # 1. Clear all documents in the collection
    await db[coll_name].delete_many({})
    
    # 2. Reset the corresponding serial counter in 'counters' collection
    # Setting seq to 0 means the next call to get_next_serial will return 1.
    if coll_name == "raw":
        await db["counters"].update_one(
            {"_id": "raw_serial"},
            {"$set": {"seq": 0}},
            upsert=True
        )
    elif coll_name == "validated":
        await db["counters"].update_one(
            {"_id": "validated_serial"},
            {"$set": {"seq": 0}},
            upsert=True
        )
            
    return {
        "status": "success",
        "message": f"Successfully cleared data and reset counters for: {coll_name}"
    }
