import asyncio
from database import get_db, connect_db, close_db

async def check():
    await connect_db()
    db = get_db()
    total = await db['raw'].count_documents({})
    validated = await db['raw'].count_documents({'validation': True})
    not_validated = await db['raw'].count_documents({'validation': {'$ne': True}})
    
    print(f"Total records in raw: {total}")
    print(f"Records with validation=True: {validated}")
    print(f"Records with validation!=True: {not_validated}")
    await close_db()

if __name__ == "__main__":
    asyncio.run(check())
