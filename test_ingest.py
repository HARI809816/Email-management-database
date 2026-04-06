"""
test_ingest.py - Quick test for ingest_client.py
Run: venv\Scripts\python test_ingest.py
"""
import os
from dotenv import load_dotenv
load_dotenv()

from ingest_client import ingest, BLOB_THRESHOLD

API_BASE = os.getenv("API_BASE", "https://email-management-database.vercel.app")

def make_records(n: int) -> list[dict]:
    return [
        {
            "name":    f"Test User {i}",
            "email":   f"testuser{i}_autotest@example.com",
            "country": "US",
        }
        for i in range(n)
    ]

def run_test(label, count):
    print(f"\n{'='*55}")
    print(f"  TEST : {label}")
    print(f"  COUNT: {count} records")
    print(f"  PATH : {'BLOB' if count > BLOB_THRESHOLD else 'DIRECT JSON'}")
    print(f"{'='*55}")
    records = make_records(count)
    try:
        result = ingest(records)
        print(f"  [OK] inserted : {result.get('inserted')}")
        print(f"  [OK] skipped  : {result.get('skipped')}")
        print(f"  [OK] message  : {result.get('message')}")
    except Exception as e:
        print(f"  [FAIL] {e}")

if __name__ == "__main__":
    print(f"\nAPI_BASE       : {API_BASE}")
    print(f"BLOB_THRESHOLD : {BLOB_THRESHOLD} records")
    print(f"BLOB TOKEN set : {'YES' if os.getenv('BLOB_READ_WRITE_TOKEN') else 'NO - check .env!'}")

    run_test("Small batch - direct JSON path", count=50)
    run_test("Large batch - blob upload path",  count=6_000)

    print("\nTests complete.\n")
