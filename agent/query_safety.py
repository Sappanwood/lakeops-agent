"""Deterministic SELECT validation and bounded, isolated DuckDB execution."""

from __future__ import annotations

import math
import multiprocessing
import pickle
import threading
import time
from collections.abc import Mapping, Set
from dataclasses import dataclass
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp


class QuerySafetyError(ValueError):
    """A stable, sanitized failure at the SQL boundary."""

    def __init__(self, code: str, detail: str = "query rejected by deterministic policy") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


@dataclass(frozen=True)
class QueryLimits:
    timeout_seconds: float = 10
    memory_bytes: int = 128 * 1024 * 1024
    process_memory_bytes: int = 1024 * 1024 * 1024
    max_scan_bytes: int = 256 * 1024 * 1024
    max_rows: int = 1000
    max_result_bytes: int = 4 * 1024 * 1024
    max_concurrency: int = 2

    def __post_init__(self) -> None:
        for name, value in vars(self).items():
            if name == "timeout_seconds":
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                    raise ValueError("timeout_seconds must be finite and positive")
            elif type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.memory_bytes > self.process_memory_bytes:
            raise ValueError("DuckDB memory budget cannot exceed worker address-space budget")


@dataclass(frozen=True)
class BoundView:
    """Host-owned SQL and conservative input bytes; never populate from model output."""

    query: str
    scan_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.query, str) or not self.query.strip() or type(self.scan_bytes) is not int or self.scan_bytes < 0:
            raise ValueError("binding requires trusted SQL and nonnegative input bytes")


@dataclass(frozen=True)
class ValidatedSql:
    sql: str
    views: tuple[str, ...]


@dataclass(frozen=True)
class SqlResult:
    sql: str
    views: tuple[str, ...]
    columns: tuple[str, ...]
    rows: list[tuple[Any, ...]]
    scan_bytes_upper_bound: int
    elapsed_seconds: float


# Exact node types fail closed as the parser grows; functions are explicit too.
_ALLOWED_NODES = frozenset({
    exp.Select, exp.From, exp.Table, exp.TableAlias, exp.Identifier, exp.Column,
    exp.Star, exp.Alias, exp.Literal, exp.Null, exp.Boolean, exp.Paren,
    exp.Where, exp.Group, exp.Having, exp.Order, exp.Ordered, exp.Limit, exp.Offset,
    exp.Join, exp.Subquery, exp.Distinct, exp.And, exp.Or, exp.Not,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Is, exp.In, exp.Between,
    exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.Neg,
    exp.Sum, exp.Count, exp.Min, exp.Max, exp.Avg, exp.Abs, exp.Round,
    exp.Coalesce, exp.Nullif, exp.Case, exp.If,
})


def validate_sql(sql: str, registered_views: Set[str]) -> ValidatedSql:
    """Accept one bounded DuckDB SELECT tree and regenerate only accepted syntax."""

    try:
        if not isinstance(sql, str) or not sql.strip() or len(sql.encode("utf-8")) > 32768:
            raise QuerySafetyError("invalid_sql")
        statements = sqlglot.parse(sql, read="duckdb", error_level=sqlglot.ErrorLevel.RAISE)
        if len(statements) != 1 or type(statements[0]) is not exp.Select:
            raise QuerySafetyError("unsupported_sql")
        tree = statements[0]
        nodes = list(tree.walk())
        if len(nodes) > 1024:
            raise QuerySafetyError("sql_complexity")
        views = []
        for node in nodes:
            if type(node) not in _ALLOWED_NODES:
                raise QuerySafetyError("unsupported_sql")
            if node.depth > 64:
                raise QuerySafetyError("sql_complexity")
            if isinstance(node, exp.Table):
                if type(node.this) is not exp.Identifier or node.db or node.catalog or node.name not in registered_views:
                    raise QuerySafetyError("unknown_view")
                views.append(node.name)
            if isinstance(node, exp.Column) and (node.db or node.catalog):
                raise QuerySafetyError("unsupported_sql")
        if not views:
            raise QuerySafetyError("unknown_view")
        return ValidatedSql(tree.sql(dialect="duckdb", comments=False, unsupported_level=sqlglot.ErrorLevel.RAISE), tuple(views))
    except (sqlglot.errors.SqlglotError, RecursionError, UnicodeError) as error:
        raise QuerySafetyError("invalid_sql") from error


class SqlExecutor:
    """One service-owned instance shares admission across its calling threads.

    Catalog identities, limits, and bindings are trusted host inputs. The only
    model-controlled argument is SQL. Separate service processes need their own
    deployment-level concurrency allocation.
    """

    def __init__(self, registered_views: Set[str], *, limits: QueryLimits = QueryLimits()) -> None:
        self._registered_views = frozenset(registered_views)
        self._limits = limits
        self._slots = threading.BoundedSemaphore(limits.max_concurrency)
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active_queries(self) -> int:
        with self._lock:
            return self._active

    def execute(self, sql: str, bindings: Mapping[str, BoundView]) -> SqlResult:
        """Revalidate SQL on every invocation; never accept a validation token."""

        if not self._slots.acquire(blocking=False):
            raise QuerySafetyError("concurrency_limit")
        with self._lock:
            self._active += 1
        started = time.monotonic()
        try:
            validated = validate_sql(sql, self._registered_views)
            selected = {}
            scan_bytes = 0
            for view in validated.views:
                binding = bindings.get(view)
                if not isinstance(binding, BoundView):
                    raise QuerySafetyError("view_unavailable")
                selected[view] = binding
                scan_bytes += binding.scan_bytes
            if scan_bytes > self._limits.max_scan_bytes:
                raise QuerySafetyError("scan_limit")
            columns, rows = self._execute_worker(validated.sql, selected, started)
            return SqlResult(validated.sql, validated.views, columns, rows, scan_bytes, time.monotonic() - started)
        finally:
            with self._lock:
                self._active -= 1
            self._slots.release()

    def _execute_worker(self, sql: str, bindings: dict[str, BoundView], started: float) -> tuple:
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
        process = context.Process(target=_query_worker, args=(send, sql, bindings, self._limits), daemon=True, name="lakeops-sql")
        try:
            process.start()
            send.close()
            remaining = self._limits.timeout_seconds - (time.monotonic() - started)
            if remaining <= 0 or not receive.poll(remaining):
                raise QuerySafetyError("time_limit")
            try:
                payload = receive.recv_bytes(self._limits.max_result_bytes + 1024)
            except (EOFError, OSError) as error:
                raise QuerySafetyError("worker_failure") from error
            if time.monotonic() - started > self._limits.timeout_seconds:
                raise QuerySafetyError("time_limit")
            status, data = pickle.loads(payload)
            if status != "ok":
                raise QuerySafetyError(data)
            return data
        finally:
            send.close()
            receive.close()
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join()
                process.close()


def _query_worker(send: Any, sql: str, bindings: dict[str, BoundView], limits: QueryLimits) -> None:
    """Run in a disposable POSIX process with no disk spill or extension loading."""

    connection = None
    try:
        import resource

        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        memory = limits.process_memory_bytes if hard == resource.RLIM_INFINITY else min(hard, limits.process_memory_bytes)
        resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
        connection = duckdb.connect(config={
            "threads": 1,
            "memory_limit": f"{limits.memory_bytes}B",
            "temp_directory": "",
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
            "python_enable_replacements": False,
        })
        for name, binding in bindings.items():
            quoted = '"' + name.replace('"', '""') + '"'
            connection.execute(f"CREATE VIEW {quoted} AS {binding.query}")
        connection.execute("SET lock_configuration = true")
        cursor = connection.execute(sql)
        columns = tuple(column[0] for column in cursor.description)
        rows = cursor.fetchmany(limits.max_rows + 1)
        if len(rows) > limits.max_rows:
            raise QuerySafetyError("row_limit")
        payload = pickle.dumps(("ok", (columns, rows)), protocol=5)
        if len(payload) > limits.max_result_bytes:
            raise QuerySafetyError("result_size_limit")
        send.send_bytes(payload)
    except (MemoryError, duckdb.OutOfMemoryException):
        send.send_bytes(pickle.dumps(("error", "memory_limit")))
    except QuerySafetyError as error:
        send.send_bytes(pickle.dumps(("error", error.code)))
    except (ImportError, AttributeError, OSError, ValueError):
        send.send_bytes(pickle.dumps(("error", "worker_setup_failure")))
    except duckdb.Error:
        send.send_bytes(pickle.dumps(("error", "query_failure")))
    finally:
        if connection is not None:
            connection.close()
        send.close()
