from __future__ import annotations

from typing import TYPE_CHECKING, Generator

from fastapi import Request

if TYPE_CHECKING:
    from oceldb import OCEL
    from oceldb_ui.session import UISession


def get_session(request: Request) -> Generator[UISession, None, None]:
    session = request.app.state.session
    with session.lock:
        yield session


def get_ocel(request: Request) -> Generator[OCEL, None, None]:
    session = request.app.state.session
    with session.lock:
        yield session.ocel
