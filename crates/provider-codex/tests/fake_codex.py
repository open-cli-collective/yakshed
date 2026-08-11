#!/usr/bin/env python3
"""Deterministic last-validated-schema App Server fake. No network or real Codex state."""

import json
import os
import sys
import threading


SCENARIO = sys.argv[1]
assert sys.argv[-1] == "app-server"
assert os.path.isabs(os.environ["CODEX_HOME"])
if len(sys.argv) > 3:
    with open(sys.argv[2], "w", encoding="utf-8") as pid_file:
        pid_file.write(str(os.getpid()))

threads = []
initialized = False
active = None
boundary_errors = set()
recorded = {}


def record_golden(value):
    if os.environ.get("YAKSHED_UPDATE_GOLDEN") != "1":
        return
    method = value.get("method")
    params = value.get("params", {})
    item = params.get("item", {})
    name = None
    if SCENARIO in ("chunked", "transport_split_batch"):
        if method == "item/agentMessage/delta" or item.get("type") == "agentMessage" or method == "turn/completed":
            name = "simple-answer"
        elif method == "item/commandExecution/outputDelta" or item.get("type") == "commandExecution":
            name = "command-execution"
        elif item.get("type") == "fileChange":
            name = "file-change"
    elif SCENARIO == "approval" and method == "item/commandExecution/requestApproval":
        name = "approval-accepted"
    elif SCENARIO == "approval" and value.get("id") == "request-0001" and "result" in value:
        name = "approval-accepted"
    elif SCENARIO == "file_approval" and method == "item/fileChange/requestApproval":
        name = "approval-declined"
    elif SCENARIO == "file_approval" and value.get("id") == "approval-declined" and "result" in value:
        name = "approval-declined"
    elif SCENARIO == "user_input" and method == "item/tool/requestUserInput":
        name = "user-input"
    elif SCENARIO == "user_input" and value.get("id") == "request-0001" and "result" in value:
        name = "user-input"
    elif method == "item/agentMessage/delta" and params.get("itemId") == "steer-1":
        name = "steer"
    elif method == "turn/completed" and params.get("turn", {}).get("status") == "interrupted":
        name = "interrupt"
    elif SCENARIO == "unknown" and method == "codex/future":
        name = "unknown-event"
    if name:
        serialized = (
            json.dumps(value, separators=(",", ":"))
            .replace("thread-1", "thread-golden")
            .replace("turn-1", "turn-golden")
        )
        if name == "approval-accepted":
            serialized = serialized.replace("request-0001", "approval-accepted")
        elif name == "user-input":
            serialized = serialized.replace("request-0001", "user-input-1")
        normalized = json.loads(serialized)
        recorded.setdefault(name, []).append(normalized)
        path = os.path.join(os.path.dirname(__file__), "..", "test-data", "golden", f"{name}.jsonl")
        with open(path, "w", encoding="utf-8") as fixture:
            for event in recorded[name]:
                fixture.write(json.dumps(event, separators=(",", ":")) + "\n")


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode() + b"\n"


def emit(value, split=False):
    record_golden(value)
    data = encoded(value)
    if split:
        offsets = (1, 3, 7, len(data))
        start = 0
        for end in offsets:
            os.write(1, data[start:end])
            start = end
    else:
        os.write(1, data)


def emit_batch(values):
    for value in values:
        record_golden(value)
    os.write(1, b"".join(encoded(value) for value in values))


def thread(thread_id, cwd, name=None):
    return {
        "cliVersion": "0.147.0",
        "createdAt": 1,
        "ephemeral": False,
        "id": thread_id,
        "cwd": cwd,
        "modelProvider": "fake",
        "name": name,
        "preview": name or "Codex thread",
        "sessionId": f"session-{thread_id}",
        "source": "appServer",
        "status": {"type": "idle"},
        "turns": [],
        "updatedAt": 1,
    }


def session_response(value):
    return {
        "approvalPolicy": "on-request",
        "approvalsReviewer": None,
        "cwd": value["cwd"],
        "model": "fake-model",
        "modelProvider": "fake",
        "sandbox": {"type": "dangerFullAccess"},
        "thread": value,
    }


def terminal(status="completed"):
    emit(
        {
            "method": "turn/completed",
            "params": {
                "threadId": active[0],
                "turn": {"id": active[1], "status": status, "items": []},
            },
        }
    )


def run_events():
    thread_id, turn_id = active
    common = {"threadId": thread_id, "turnId": turn_id}
    if SCENARIO in ("chunked", "transport_split_batch"):
        emit_batch(
            [
                {
                    "method": "item/agentMessage/delta",
                    "params": {**common, "itemId": "message-1", "delta": "hel"},
                },
                {
                    "method": "item/agentMessage/delta",
                    "params": {**common, "itemId": "message-1", "delta": "lo"},
                },
                {
                    "method": "item/completed",
                    "params": {
                        **common,
                        "completedAtMs": 1,
                        "item": {"id": "message-1", "type": "agentMessage", "text": "hello"},
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        **common,
                        "completedAtMs": 2,
                        "item": {
                            "id": "file-1",
                            "type": "fileChange",
                            "status": "completed",
                            "changes": [
                                {"path": "src/main.rs", "kind": "update", "diff": "updated"}
                            ],
                        },
                    },
                },
                {
                    "method": "item/started",
                    "params": {
                        **common,
                        "startedAtMs": 3,
                        "item": {
                            "id": "command-1",
                            "type": "commandExecution",
                            "command": "cargo test",
                        },
                    },
                },
                {
                    "method": "item/commandExecution/outputDelta",
                    "params": {**common, "itemId": "command-1", "delta": "ok"},
                },
                {
                    "method": "item/completed",
                    "params": {
                        **common,
                        "completedAtMs": 4,
                        "item": {
                            "id": "command-1",
                            "type": "commandExecution",
                            "command": "cargo test",
                            "aggregatedOutput": "ok",
                        },
                    },
                },
                {
                    "method": "turn/completed",
                    "params": {
                        "threadId": thread_id,
                        "turn": {"id": turn_id, "status": "completed", "items": []},
                    },
                },
            ]
        )
    elif SCENARIO in ("approval", "approval_declined", "shutdown_settlement"):
        emit_batch(
            [
                {
                    "id": "request-0001",
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        **common,
                        "itemId": "command-1",
                        "startedAtMs": 1,
                        "command": "cargo test",
                        "reason": "run command",
                    },
                },
                {
                    "method": "item/agentMessage/delta",
                    "params": {**common, "itemId": "message-1", "delta": "reader-still-live"},
                },
            ]
        )
    elif SCENARIO == "file_approval":
        fixture_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "test-data",
            "golden",
            "approval-declined.jsonl",
        )
        with open(fixture_path, encoding="utf-8") as fixture:
            request = json.loads(next(fixture))
        request["params"].update(common)
        emit(request)
    elif SCENARIO == "response_disconnect":
        os.close(sys.stdin.fileno())
        emit(
            {
                "id": "request-0001",
                "method": "item/commandExecution/requestApproval",
                "params": {
                    **common,
                    "itemId": "command-1",
                    "startedAtMs": 1,
                    "command": "cargo test",
                },
            }
        )
        threading.Event().wait()
    elif SCENARIO == "request_boundary":
        emit_batch(
            [
                {
                    "id": "unknown-request",
                    "method": "codex/future/request",
                    "params": common,
                },
                {
                    "id": "malformed-request",
                    "method": "item/tool/requestUserInput",
                    "params": {**common, "itemId": "input-1"},
                },
            ]
        )
    elif SCENARIO == "uncorrelated_identity":
        unknown = {"threadId": "thread-unknown", "turnId": "turn-unknown"}
        emit_batch(
            [
                {
                    "method": "item/agentMessage/delta",
                    "params": {**unknown, "itemId": "message-1", "delta": "wrong-run"},
                },
                {
                    "id": "uncorrelated-approval",
                    "method": "item/commandExecution/requestApproval",
                    "params": {
                        **unknown,
                        "itemId": "command-1",
                        "startedAtMs": 1,
                        "command": "danger",
                    },
                },
            ]
        )
    elif SCENARIO == "structural_malformed":
        emit(
            {
                "method": "item/agentMessage/delta",
                "params": {**common, "itemId": "message-1"},
            }
        )
        terminal()
    elif SCENARIO == "malformed_terminal":
        emit(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": thread_id,
                    "turn": {"id": turn_id, "items": []},
                },
            }
        )
    elif SCENARIO == "user_input":
        emit(
            {
                "id": "request-0001",
                "method": "item/tool/requestUserInput",
                "params": {
                    **common,
                    "itemId": "input-1",
                    "isBlocking": True,
                    "questions": [
                        {"id": "color", "header": "Color", "question": "favorite color?"}
                    ],
                },
            }
        )
    elif SCENARIO == "unknown":
        emit(
            {
                "method": "codex/future",
                "params": {**common, "answer": 42},
            }
        )
    elif SCENARIO == "malformed":
        if os.environ.get("YAKSHED_UPDATE_GOLDEN") == "1":
            path = os.path.join(os.path.dirname(__file__), "..", "test-data", "golden", "malformed-frame.jsonl")
            with open(path, "w", encoding="utf-8") as fixture:
                fixture.write("{not-json\n")
        os.write(1, b"{not-json\n")
        terminal()
    elif SCENARIO == "canary_event":
        emit_batch(
            [
                {
                    "method": "codex/native",
                    "params": {
                        **common,
                        "credential": "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT",
                    },
                },
                {
                    "method": "item/agentMessage/delta",
                    "params": {
                        **common,
                        "itemId": "message-canary",
                        "delta": "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT",
                    },
                },
            ]
        )
        terminal()
    elif SCENARIO == "stderr_flood":
        os.write(
            2,
            (
                "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT\n"
                + "diagnostic\n" * 20000
            ).encode(),
        )
        terminal()
    elif SCENARIO == "oversized":
        os.write(1, ("{\"method\":\"future/huge\",\"padding\":\"" + "x" * 4096 + "\"}\n").encode())
        terminal()


for line in sys.stdin:
    message = json.loads(line)
    record_golden(message)
    method = message.get("method")
    request_id = message.get("id")

    if method == "initialize":
        if SCENARIO == "disconnected":
            sys.exit(0)
        if SCENARIO == "canary_error":
            emit(
                {
                    "id": request_id,
                    "error": {
                        "code": -32000,
                        "message": "YAKSHED_CREDENTIAL_CANARY_DO_NOT_EMIT protocol failure",
                    },
                }
            )
            continue
        if SCENARIO == "overloaded":
            emit(
                {
                    "id": request_id,
                    "error": {"code": -32001, "message": "serverOverloaded"},
                }
            )
            continue
        if SCENARIO == "malformed_initialize":
            emit({"id": request_id, "result": {}})
            continue
        if SCENARIO == "initialize_home_mismatch":
            emit(
                {
                    "id": request_id,
                    "result": {
                        "codexHome": os.environ["CODEX_HOME"] + "-mismatch",
                        "platformFamily": "unix",
                        "platformOs": "test",
                        "userAgent": "fake-codex",
                    },
                },
                split=SCENARIO in ("chunked", "transport_split_batch"),
            )
            continue
        emit(
            {
                "id": request_id,
                "result": {
                    "codexHome": os.environ["CODEX_HOME"],
                    "platformFamily": "unix",
                    "platformOs": "test",
                    "userAgent": "fake-codex",
                },
            },
            split=SCENARIO in ("chunked", "transport_split_batch"),
        )
    elif method == "initialized":
        initialized = True
    else:
        assert initialized, "request arrived before initialized notification"
        if method == "thread/start":
            thread_id = f"thread-{len(threads) + 1}"
            value = thread(thread_id, message["params"]["cwd"])
            threads.append(value)
            emit(
                {"id": request_id, "result": session_response(value)},
                split=SCENARIO == "transport_split_batch",
            )
        elif method == "thread/name/set":
            selected = next(item for item in threads if item["id"] == message["params"]["threadId"])
            selected["name"] = message["params"]["name"]
            selected["preview"] = selected["name"]
            if SCENARIO == "malformed_name_ack":
                emit({"id": request_id, "result": {"unexpected": True}})
            else:
                emit({"id": request_id, "result": {}})
        elif method == "thread/list":
            if SCENARIO == "client_write_failure":
                os.close(sys.stdin.fileno())
                emit(
                    {
                        "method": "test/clientRequestPending",
                        "params": {"threadId": active[0], "turnId": active[1]},
                    }
                )
                threading.Event().wait()
            start = int(message["params"].get("cursor") or 0)
            limit = message["params"]["limit"]
            page = threads[start : start + limit]
            next_cursor = str(start + limit) if start + limit < len(threads) else None
            emit({"id": request_id, "result": {"data": page, "nextCursor": next_cursor}})
        elif method == "thread/resume":
            selected = next(item for item in threads if item["id"] == message["params"]["threadId"])
            if SCENARIO == "resume_mismatched_thread":
                selected = selected.copy()
                selected["id"] = "thread-mismatch"
            emit({"id": request_id, "result": session_response(selected)})
        elif method == "turn/start":
            if SCENARIO == "shutdown_settlement" and active is not None:
                emit(
                    {
                        "method": "test/secondTurnReceived",
                        "params": {"threadId": active[0], "turnId": active[1]},
                    }
                )
                continue
            active = (message["params"]["threadId"], "turn-1")
            if SCENARIO == "early_before_ack":
                sys.exit(0)
            if SCENARIO == "malformed_turn_ack":
                emit({"id": request_id, "result": {}})
                continue
            emit(
                {"id": request_id, "result": {"turn": {"id": active[1], "status": "inProgress", "items": []}}},
                split=SCENARIO == "transport_split_batch",
            )
            if SCENARIO == "crash":
                sys.exit(7)
            run_events()
        elif method == "turn/steer":
            if SCENARIO == "malformed_steer_ack":
                emit({"id": request_id, "result": {}})
                continue
            if SCENARIO == "mismatched_steer_ack":
                emit({"id": request_id, "result": {"turnId": "turn-other"}})
                continue
            emit({"id": request_id, "result": {"turnId": active[1]}})
            emit(
                {
                    "method": "item/agentMessage/delta",
                    "params": {
                        "threadId": active[0],
                        "turnId": active[1],
                        "itemId": "steer-1",
                        "delta": message["params"]["input"][0]["text"],
                    },
                }
            )
        elif method == "turn/interrupt":
            if SCENARIO == "malformed_interrupt_ack":
                emit({"id": request_id, "result": {"unexpected": True}})
                continue
            emit({"id": request_id, "result": {}})
            terminal("interrupted")
        elif request_id == "request-0001":
            if SCENARIO in ("approval", "approval_declined"):
                expected = "decline" if SCENARIO == "approval_declined" else "accept"
                assert message["result"]["decision"] == expected
                terminal()
            elif SCENARIO == "user_input":
                assert message["result"]["answers"]["color"]["answers"] == ["blue"]
                emit(
                    {
                        "method": "item/agentMessage/delta",
                        "params": {
                            "threadId": active[0],
                            "turnId": active[1],
                            "itemId": "message-1",
                            "delta": "input-accepted",
                        },
                    }
                )
                terminal()
        elif SCENARIO == "request_boundary" and request_id in (
            "unknown-request",
            "malformed-request",
        ):
            expected = -32601 if request_id == "unknown-request" else -32602
            assert message["error"]["code"] == expected
            boundary_errors.add(request_id)
            if len(boundary_errors) == 2:
                terminal()
        elif SCENARIO == "uncorrelated_identity" and request_id == "uncorrelated-approval":
            assert message["error"]["code"] == -32602
            terminal()
        elif SCENARIO == "file_approval" and request_id == "approval-declined":
            assert message["result"]["decision"] == "decline"
            terminal()
        else:
            emit({"id": request_id, "error": {"code": -32601, "message": "unknown method"}})
