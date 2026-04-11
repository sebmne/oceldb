from __future__ import annotations

import threading
import webbrowser
from pathlib import Path

import uvicorn

from oceldb import OCEL
from oceldb.io import read_ocel

from oceldb_ui.server import create_app
from oceldb_ui.session import UISession


def open_ui(ocel_or_path: OCEL | str | Path, *, port: int = 8765) -> None:
    if isinstance(ocel_or_path, OCEL):
        ocel = ocel_or_path
    else:
        ocel = read_ocel(ocel_or_path)

    session = UISession(ocel=ocel)
    app = create_app(session)

    url = f"http://localhost:{port}"
    threading.Timer(1.0, webbrowser.open, args=[url]).start()

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
