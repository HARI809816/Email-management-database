import asyncio
from database import get_db, connect_db, close_db

async def reset():
    await connect_db()
    db = get_db()
    res = await db['raw'].update_many(
        {'serial_no': {'$lte': 10000}},
        {'$set': {'validation': False}}
    )
    print(f"Successfully reset {res.modified_count} records to validation=False.")
    await close_db()

if __name__ == "__main__":
    asyncio.run(reset())
