"""Explicit process-local instrumentation. No initialization or export on import."""

# Export/instrumentation failures are deliberately isolated without diagnostic text.
# ruff: noqa: BLE001, S110

import atexit
import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable, Sequence
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.metrics import AlwaysOffExemplarFilter, MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricExportResult,
    MetricsData,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExportResult,
)
from opentelemetry.trace import Status, StatusCode
from sqlalchemy.exc import SQLAlchemyError

P = ParamSpec("P")
T = TypeVar("T")
COMPONENTS = {"api", "worker", "dispatcher", "operator", "diagnostics"}
# All telemetry dimensions and instrument names are defined here, never from data.
COUNTERS = {
    ("api", "submit"): "task_engine.job.submissions",
    ("worker", "claim"): "task_engine.job.claims",
    ("worker", "finish"): "task_engine.job.transitions",
    ("worker", "recover"): "task_engine.job.recoveries",
    ("worker", "handle"): "task_engine.job.executions",
    ("dispatcher", "claim"): "task_engine.delivery.attempts",
    ("dispatcher", "finish"): "task_engine.delivery.transitions",
    ("dispatcher", "recover"): "task_engine.delivery.recoveries",
    ("dispatcher", "receive"): "task_engine.delivery.receives",
    ("operator", "replay"): "task_engine.operator.replays",
}
OPERATIONS = set(COUNTERS) | {("api", "lookup")}
OUTCOMES = {
    "completed", "created", "replayed", "conflict", "invalid_input", "found", "not_found",
    "claimed", "empty", "succeeded", "failed", "retry", "dead_letter", "acknowledged",
    "reclaimed", "lease_lost", "retryable_error", "permanent_error", "unexpected_error",
    "database_error", "interrupted", "invalid_budget", "event_not_found", "event_not_dead_letter",
    "budget_or_counter_overflow", "observed",
}
ERROR_CODES = {
    "retryable_error", "permanent_error", "unexpected_error", "database_error", "lease_expired",
}
DURATIONS = {
    ("worker", "handle"): "task_engine.handler.duration",
    ("dispatcher", "receive"): "task_engine.receiver.duration",
}


class SafeConsoleSpanExporter(ConsoleSpanExporter):
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        try:
            return super().export(spans)
        except Exception:
            # Never let stdout/export failures affect work or log exception diagnostics.
            return SpanExportResult.FAILURE


class SafeConsoleMetricExporter(ConsoleMetricExporter):
    def export(
        self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any,
    ) -> MetricExportResult:
        try:
            return super().export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        except Exception:
            return MetricExportResult.FAILURE


class Telemetry:
    """Private providers avoid global-provider replacement and ambient resource detectors."""

    def __init__(self, traces: TracerProvider, metrics: MeterProvider) -> None:
        self.traces = traces
        self.metrics = metrics
        self.tracer = traces.get_tracer("task_engine")
        meter = metrics.get_meter("task_engine")
        self.counters = {key: meter.create_counter(name) for key, name in COUNTERS.items()}
        self.durations = {
            key: meter.create_histogram(
                name, unit="s",
                explicit_bucket_boundaries_advisory=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60),
            ) for key, name in DURATIONS.items()
        }

    def shutdown(self) -> None:
        for provider in (self.traces, self.metrics):
            try:
                provider.shutdown()
            except Exception:
                pass


_runtime: Telemetry | None = None
_active: ContextVar[tuple[str, str] | None] = ContextVar("telemetry_operation", default=None)
_initialized = False
_lock = threading.Lock()


def initialize(component: str) -> None:
    """Call at process startup; first call wins, including disabled mode."""
    global _initialized, _runtime
    with _lock:
        if _initialized:
            return
        if component not in COMPONENTS:
            raise ValueError("invalid_telemetry_component")
        mode = os.getenv("TASK_ENGINE_TELEMETRY", "disabled")
        if mode not in {"disabled", "console"}:
            raise ValueError("invalid_telemetry_mode")
        if mode == "console":
            resource = Resource({"service.name": f"task-engine-{component}"})
            traces = TracerProvider(resource=resource, shutdown_on_exit=False)
            traces.add_span_processor(SimpleSpanProcessor(SafeConsoleSpanExporter(out=sys.stdout)))
            reader = PeriodicExportingMetricReader(
                SafeConsoleMetricExporter(out=sys.stdout), export_interval_millis=10_000,
            )
            metrics = MeterProvider(
                resource=resource, metric_readers=[reader], shutdown_on_exit=False,
                exemplar_filter=AlwaysOffExemplarFilter(),
            )
            _runtime = Telemetry(traces, metrics)
            atexit.register(_runtime.shutdown)
        _initialized = True


def _attributes(
    component: str, operation: str, outcome: str, error_code: str | None = None,
) -> dict[str, str]:
    if (component, operation) not in OPERATIONS or outcome not in OUTCOMES:
        raise ValueError("invalid_telemetry_vocabulary")
    attrs = {"component": component, "operation": operation, "outcome": outcome}
    if error_code is not None:
        if error_code not in ERROR_CODES:
            raise ValueError("invalid_telemetry_error_code")
        attrs["error_code"] = error_code
    return attrs


def outcome(component: str, operation: str, value: str) -> None:
    try:
        if _active.get() == (component, operation):
            trace.get_current_span().set_attributes(_attributes(component, operation, value))
    except Exception:
        pass


def record(
    component: str, operation: str, value: str, *, count: int = 1,
    error_code: str | None = None,
) -> None:
    """Transition callers must invoke ONLY after confirmed commit, outside transactions."""
    try:
        attrs = _attributes(component, operation, value, error_code)
        if _active.get() == (component, operation):
            trace.get_current_span().set_attributes(attrs)
        if _runtime is not None:
            _runtime.counters[component, operation].add(count, attrs)
        logging.getLogger(f"task_engine.{component}").info(
            "%s: %s", operation, value, extra=attrs | {"event": operation},
        )
    except Exception:
        pass  # Instrumentation must never change a confirmed database outcome.


def _error_outcome(error: BaseException) -> str:
    if isinstance(error, SQLAlchemyError):
        return "database_error"
    if not isinstance(error, Exception):
        return "interrupted"
    # Only predefined names/codes are exported, never repr/str/type of arbitrary exceptions.
    return {
        "LeaseLostError": "lease_lost", "DeliveryLeaseLostError": "lease_lost",
        "RetryableJobError": "retryable_error", "RetryableDeliveryError": "retryable_error",
        "PermanentJobError": "permanent_error", "PermanentDeliveryError": "permanent_error",
    }.get(type(error).__name__, "unexpected_error")


def traced(component: str, operation: str) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Bounded spans; automatic exception events/status descriptions are disabled."""
    def decorate(function: Callable[P, T]) -> Callable[P, T]:
        @wraps(function)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            span: trace.Span = trace.INVALID_SPAN
            if _runtime is not None:
                try:
                    span = _runtime.tracer.start_span(
                        f"{component}.{operation}",
                        attributes=_attributes(component, operation, "observed"),
                    )
                except Exception:
                    pass
            active_token = _active.set((component, operation))
            started = time.perf_counter()
            result = "succeeded"
            try:
                with trace.use_span(span, record_exception=False, set_status_on_exception=False):
                    try:
                        return function(*args, **kwargs)
                    except BaseException as error:
                        result = _error_outcome(error)
                        # ReplayError is explicitly bounded too; no arbitrary code is exported.
                        if type(error).__name__ == "ReplayError":
                            code = getattr(error, "code", None)
                            if code in OUTCOMES:
                                result = code
                        try:
                            span.set_attributes(_attributes(component, operation, result))
                            span.set_status(Status(StatusCode.ERROR))
                        except Exception:
                            pass
                        if (component, operation) == ("operator", "replay"):
                            record(component, operation, result)
                        raise
                    finally:
                        if (component, operation) in DURATIONS:
                            record(component, operation, result)
                            if _runtime is not None:
                                try:
                                    _runtime.durations[component, operation].record(
                                        time.perf_counter() - started,
                                        _attributes(component, operation, result),
                                    )
                                except Exception:
                                    pass
            finally:
                _active.reset(active_token)
                try:
                    span.end()
                except Exception:
                    pass
        return wrapped
    return decorate


class JsonFormatter(logging.Formatter):
    """Allowlisted fields only; ignores message, args, exc_info and stack_info."""

    def format(self, record: logging.LogRecord) -> str:
        component = record.name.removeprefix("task_engine.")
        if component not in COMPONENTS:
            component = "runtime"
        event = getattr(record, "event", "diagnostic")
        if (component, event) not in OPERATIONS:
            event = "diagnostic"
        value = getattr(record, "outcome", "observed")
        if value not in OUTCOMES:
            value = "observed"
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname, "component": component, "event": event, "outcome": value,
        })


def configure_logging(*, level: int = logging.INFO) -> None:
    json_logs = os.getenv("TASK_ENGINE_LOG_FORMAT", "human") == "json"
    logging.basicConfig(level=logging.INFO if json_logs else level)
    if json_logs:
        formatter = JsonFormatter()
        for name in ("", "uvicorn", "uvicorn.error", "uvicorn.access"):
            for handler in logging.getLogger(name).handlers:
                handler.setFormatter(formatter)
