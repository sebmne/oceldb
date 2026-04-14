from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_smoke_example_script_runs() -> None:
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(root / "src")

    completed = subprocess.run(
        [sys.executable, str(root / "examples" / "smoke.py")],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = completed.stdout
    assert "events=" in stdout
    assert "objects=" in stdout
    assert "event_type_counts=" in stdout
    assert "timeline=" in stdout
    assert "dfg_nodes=" in stdout
