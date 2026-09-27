"""PyInstaller entry point for the packaged YakShed service."""

from backend.yakshed.__main__ import main


if __name__ == "__main__":
    raise SystemExit(main())
