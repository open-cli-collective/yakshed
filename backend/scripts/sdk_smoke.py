"""Initialize the pinned Codex runtime without making a model call.

Run this only in an isolated temporary directory. The script reports only
whether the delegated account is present; it never prints account details or
reads credential files.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import tempfile

from openai_codex.client import CodexClient, CodexConfig


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cwd", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="yakshed-sdk-smoke-") as temporary:
        cwd = args.cwd or Path(temporary)
        client = CodexClient(CodexConfig(cwd=str(cwd), client_name="yakshed_sdk_smoke"), approval_handler=lambda _method, _params: {"decision": "decline"})
        try:
            client.start()
            initialize = client.initialize()
            account = client.account_read()
            print({"server_version": initialize.serverInfo.version if initialize.serverInfo else None, "account_present": account.account is not None})
        finally:
            client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
