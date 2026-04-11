from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from oceldb_ui.routes import browse, overview, process_map, sql
from oceldb_ui.session import UISession

_STATIC_DIR = Path(__file__).parent / "webapp" / "dist"


def create_app(session: UISession) -> FastAPI:
    app = FastAPI(title="oceldb UI", version="0.2.0")
    app.state.session = session

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(overview.router)
    app.include_router(browse.router)
    app.include_router(sql.router)
    app.include_router(process_map.router)

    if _STATIC_DIR.is_dir():
        app.mount("/", StaticFiles(directory=_STATIC_DIR, html=True), name="frontend")

    return app
