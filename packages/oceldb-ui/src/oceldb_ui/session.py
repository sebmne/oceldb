from __future__ import annotations

import threading
from dataclasses import dataclass, field

from oceldb import OCEL


@dataclass
class SubLog:
    id: str
    ocel: OCEL
    root: str
    types: list[str]
    filter_count: int


@dataclass
class UISession:
    ocel: OCEL
    sublogs: dict[str, SubLog] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
