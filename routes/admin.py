from fastapi import APIRouter
from enum import Enum
from database import get_db

router = APIRouter(prefix="/admin", tags=["Admin"])

class CollectionEnum(str, Enum):
    raw = "raw"
    validated = "validated"
    # history = "history"
    # all = "all"

@router.post("/reset")
async def reset_table(collection: CollectionEnum):
    """
    Clears the selected collection and resets its serial number counter to start from 1.
    
    Warning: This action is destructive and cannot be undone.
    """
    db = get_db()
    
    collections_to_reset = []
    if collection == CollectionEnum.all:
        collections_to_reset = ["raw", "validated", "history"]
    else:
        collections_to_reset = [collection.value]
        
    for coll in collections_to_reset:
        # 1. Clear all documents in the collection
        await db[coll].delete_many({})
        
        # 2. Reset the corresponding serial counter in 'counters' collection
        # Setting seq to 0 means the next call to get_next_serial will return 1.
        if coll == "raw":
            await db["counters"].update_one(
                {"_id": "raw_serial"},
                {"$set": {"seq": 0}},
                upsert=True
            )
        elif coll == "validated":
            await db["counters"].update_one(
                {"_id": "validated_serial"},
                {"$set": {"seq": 0}},
                upsert=True
            )
            
    return {
        "status": "success",
        "message": f"Successfully cleared data and reset counters for: {collection.value}"
    }
