# Electron desktop runbook

The active desktop shell is Electron. The renderer talks to the local Python
service through the context-isolated preload bridge; production builds do not
open a localhost API.

Install the locked Node dependencies and start an empty development app:

```sh
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r backend/requirements.txt
export YAKSHED_PYTHON="$PWD/.venv/bin/python"
npm ci
npm run dev
```

The isolated fixture is opt-in:

```sh
npm run dev:demo
```

For browser-only UI checks with the real demo JSONL service, build the
renderer and start the loopback preview:

```sh
npm run browser-preview
```

The preview binds to `127.0.0.1`, requires its per-process CSRF token, rejects
live provider connections and login, and removes its temporary SQLite root on
exit. It is test-only and is never used by packaged production startup.

The renderer build is written to `desktop/frontend`. The packaged service is
staged at `backend/dist/yakshed-service` by the pinned PyInstaller step. On a
machine with Python 3.12 or 3.13 and PyInstaller installed, run:

```sh
python scripts/freeze_service.py --output backend/dist/yakshed-service
```

The freeze smoke sends a snapshot through the executable and asks the bundled
Codex runtime for account status without making a model call. After staging
the service and renderer, build and smoke-test the app:

```sh
npm run package
npm run package:smoke
```

Release automation creates draft artifacts and checksums. Public publishing
requires the configured macOS signing and notarization secrets; unsigned
artifacts remain drafts.
