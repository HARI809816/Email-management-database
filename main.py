from fastapi import FastAPI
from contextlib import asynccontextmanager

from database import connect_db, close_db
from routes import ingest, raw, validate, validated, history, admin


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    await connect_db()
    yield
    await close_db()


app = FastAPI(
    title="Data Manager API",
    description="Manages raw ingestion, validation, and history logging with MongoDB.",
    version="1.0.0",
    lifespan=lifespan
)

# ── Register all routers ──────────────────────────────────────────────────────
app.include_router(ingest.router)
app.include_router(raw.router)
app.include_router(validate.router)
app.include_router(validated.router)
app.include_router(history.router)
app.include_router(admin.router)


@app.get("/", tags=["Health"])
async def root():
    return {
        "status":  "Data Manager API is running",
        "docs":    "/docs",
        "redoc":   "/redoc"
    }
