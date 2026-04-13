"""
FastAPI application entry point.

Layer: API / Entry
Run with:
    cd backend
    uvicorn app.main:app --reload --port 8000

The API will be available at http://localhost:8000
Interactive docs at http://localhost:8000/docs
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.middleware import RequestIdMiddleware
from app.routes.festival import router as festival_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = FastAPI(
    title="Festival Planning Tool",
    description=(
        "Automates festival impact analysis across 5 hierarchical levels: "
        "City, City-SubCat, City-SubCat-CutClass, City-Hub, "
        "City-Hub-SubCat-CutClass.  Provides editable override fields "
        "with auto-cascading re-indexing and Excel export."
    ),
    version="1.0.0",
)

app.add_middleware(RequestIdMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
)

# Register all festival planning routes under /festivals
app.include_router(festival_router)


@app.on_event("startup")
def _log_pyarrow_version() -> None:
    """So logs show which PyArrow is active (need >=19.0.1 for parquet reads)."""
    try:
        import pyarrow as pa

        logging.getLogger("app.main").info("pyarrow version: %s", pa.__version__)
    except Exception:
        pass


@app.get("/health")
def health():
    """
    Health check endpoint.

    Returns
    -------
    dict
        {"status": "ok"} if the server is running.
    """
    return {"status": "ok"}
