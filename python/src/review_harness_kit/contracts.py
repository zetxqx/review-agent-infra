from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import entry_points
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

from review_harness_kit.gen.review.v1 import review_pb2

CacheShape = Literal["append_only", "single_shot", "sliding_window"]
RawOutputKind = Literal["json", "yaml", "markdown", "text"]


class HarnessUnsupportedError(Exception):
    """Raised by HarnessAdapter.prepare when the shard lacks prerequisites required by the harness."""


HarnessUnsupported = HarnessUnsupportedError


@dataclass(frozen=True)
class RunContext:
    """In-memory credentials, writable scratch paths, and rendered prompts passed to a HarnessAdapter."""

    base_url: str
    api_key: str
    home: Path
    prompt: str
    rules_text: str
    schema_path: Path


@dataclass(frozen=True)
class HarnessInvocation:
    """Subprocess execution specification returned by HarnessAdapter.prepare."""

    argv: list[str]
    cwd: str
    env: dict[str, str] = field(default_factory=dict)
    stdin_text: str | None = None


@dataclass(frozen=True)
class ProcessResult:
    """Observed subprocess exit status, timeout flag, and combined stdout/stderr log path."""

    exit_code: int | None
    timed_out: bool
    log_path: Path


@dataclass(frozen=True)
class RawOutput:
    """Unnormalized output artifact collected after a harness subprocess finishes or times out."""

    kind: RawOutputKind
    text: str


@runtime_checkable
class HarnessAdapter(Protocol):
    """Plugin boundary adapting an unmodified coding harness CLI to AgentTask and ReviewReport."""

    name: str
    cache_shape: CacheShape

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        """Build subprocess arguments, working directory, and offline environment without executing the harness."""
        ...

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        """Read raw output files or log tail produced by the harness subprocess."""
        ...

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        """Deterministically parse structured harness output into ReviewReport, or return None for LLM normalization."""
        ...


def load_adapter(name: str) -> HarnessAdapter:
    """Load and instantiate a registered HarnessAdapter plugin from entry-point group 'review_agent.harness'."""
    normalized = name.strip()
    candidates = [normalized, normalized.replace("_", "-"), normalized.replace("-", "_")]
    for candidate in dict.fromkeys(candidates):
        if not candidate:
            continue
        matches = entry_points(group="review_agent.harness", name=candidate)
        if matches:
            adapter_cls = next(iter(matches)).load()
            return adapter_cls()
    raise LookupError(f"no harness adapter registered for {name!r}")
