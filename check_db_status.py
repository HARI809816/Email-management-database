from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import os
from dotenv import load_dotenv
import certifi

load_dotenv()

async def check_counts():
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME", "EmailDataBase")
    client = AsyncIOMotorClient(uri, tlsCAFile=certifi.where())
    db = client[db_name]
    
    raw_count = await db["raw"].count_documents({})
    val_count = await db["validated"].count_documents({})
    
    print(f"Total Raw Records: {raw_count}")
    print(f"Total Validated Records: {val_count}")
    
    # Check top 5 validated emails to see if they match what user might be trying
    cursor = db["validated"].find({}, {"email": 1, "_id": 0}).limit(5)
    emails = await cursor.to_list(length=5)
    print("Sample Validated Emails:")
    for e in emails:
        print(f" - {e.get('email')}")
        
    client.close()

if __name__ == "__main__":
    asyncio.run(check_counts())
