from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from .service import Service, serve
from .providers.codex import CodexAdapter


def main() -> int:
    parser = argparse.ArgumentParser(description="YakShed provider-neutral backend")
    parser.add_argument("--data-dir", type=Path, default=Path.home() / "Library/Application Support/YakShed")
    parser.add_argument("--demo", action="store_true", help="enable the isolated deterministic demo adapter")
    args = parser.parse_args()
    service = Service(args.data_dir, demo=args.demo, secrets=CodexAdapter.secret_values(), workspace_boundary=os.environ.get("YAKSHED_WORKSPACE_ROOT"))
    serve(service, sys.stdin, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
