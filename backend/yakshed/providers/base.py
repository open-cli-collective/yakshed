from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


@dataclass(frozen=True)
class AdapterDescriptor:
    id: str
    name: str
    capabilities: tuple[str, ...] = ()
    models: tuple[str, ...] = ()


@dataclass
class RunContext:
    run_id: str
    session_id: str
    task_id: str
    prompt: str
    workspace: str
    native_id: str | None = None
    adapter_id: str = ""
    provider: str = ""
    connection_id: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class AdapterEvent:
    """Normalized provider event crossing into the product service."""

    kind: str
    native_key: str
    payload: dict[str, Any] = field(default_factory=dict)


Emit = Callable[[AdapterEvent], None]


class Adapter(Protocol):
    descriptor: AdapterDescriptor

    def start(self, context: RunContext, emit: Emit) -> None:
        ...

    def interrupt(self, run_id: str) -> None:
        ...

    def respond(self, approval_id: str, decision: str, answers: dict[str, Any] | None = None) -> None:
        ...

    def close(self) -> None:
        ...
