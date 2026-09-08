"""Adversarial tests for the generated SQL execution boundary."""

import multiprocessing
from pathlib import Path
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from agent.query_safety import BoundView, QueryLimits, QuerySafetyError, SqlExecutor, validate_sql


VIEWS = {"traffic"}
BINDINGS = {"traffic": BoundView("SELECT i AS views FROM range(10) t(i)", 100)}


class SqlValidationTests(unittest.TestCase):
    def test_read_only_analytics(self):
        for sql in (
            "SELECT * FROM traffic",
            'SELECT SUM(views) AS total FROM "traffic" WHERE views > 2',
            "SELECT a.views FROM traffic a JOIN traffic b ON a.views = b.views ORDER BY a.views LIMIT 2",
            "SELECT COUNT(DISTINCT views) FROM traffic",
            "SELECT views FROM (SELECT views FROM traffic) t WHERE views IN (1, 2)",
            "SELECT CASE WHEN views > 2 THEN 1 ELSE 0 END FROM traffic",
            "SELECT * FROM traffic /* ; DELETE FROM traffic */",
        ):
            with self.subTest(sql=sql):
                self.assertIn("traffic", validate_sql(sql, VIEWS).views)

    def test_rejects_mutation_and_bypasses(self):
        for sql in (
            "DELETE FROM traffic", "SELECT * FROM traffic; SELECT * FROM traffic",
            "COPY traffic TO '/tmp/leak'", "INSTALL httpfs", "PRAGMA version",
            "SELECT * INTO stolen FROM traffic", "ATTACH '/tmp/db' AS x",
            "SELECT * FROM read_parquet('/tmp/private')", "SELECT * FROM '/tmp/private.parquet'",
            "SELECT * FROM query('SELECT * FROM traffic')", "SELECT * FROM query_table('traffic')",
            "SELECT * FROM information_schema.tables", "SELECT * FROM main.traffic",
            "SELECT * FROM missing", "SELECT * FROM (SELECT * FROM missing) t",
            "SELECT nextval('secret') FROM traffic", "SELECT getenv('HOME') FROM traffic",
            "SELECT current_setting('home_directory') FROM traffic",
            "SELECT read_blob('/tmp/private') FROM traffic", "SELECT repeat('x', 1000000) FROM traffic",
            "SELECT list(views) FROM traffic", "SELECT * FROM duckdb_settings()",
            "WITH traffic AS (SELECT * FROM missing) SELECT * FROM traffic",
            "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT 1 FROM x) SELECT * FROM x",
            "SELECT 1", "SELECT * FROM traffic UNION ALL SELECT * FROM traffic",
            "SELECT traffic.views.foo() FROM traffic", "SELECT $1 FROM traffic",
            "SELECT * FROM traffic USING SAMPLE 100%", "", "-- comment only",
        ):
            with self.subTest(sql=sql), self.assertRaises(QuerySafetyError):
                validate_sql(sql, VIEWS)

    def test_sql_size_and_complexity_are_bounded(self):
        for sql in ("SELECT '" + "x" * 40000 + "' FROM traffic", "SELECT " + "(" * 200 + "views" + ")" * 200 + " FROM traffic"):
            with self.assertRaises(QuerySafetyError):
                validate_sql(sql, VIEWS)

    def test_isolated_surrogate_is_rejected_with_stable_error(self):
        with self.assertRaises(QuerySafetyError) as error:
            validate_sql("SELECT '\ud800' FROM traffic", VIEWS)
        self.assertEqual(error.exception.code, "invalid_sql")


class SqlExecutionTests(unittest.TestCase):
    def executor(self, **limits):
        return SqlExecutor(VIEWS, limits=QueryLimits(**limits))

    def test_isolated_surrogate_releases_slot_and_allows_next_query(self):
        executor = self.executor(max_concurrency=1)
        with self.assertRaises(QuerySafetyError) as error:
            executor.execute("SELECT '\ud800' FROM traffic", BINDINGS)
        self.assertEqual(error.exception.code, "invalid_sql")
        self.assertEqual(executor.active_queries, 0)
        self.assertEqual(executor.execute("SELECT count(*) FROM traffic", BINDINGS).rows, [(10,)])

    def test_executes_only_revalidated_sql_and_returns_bounded_rows(self):
        executor = self.executor()
        result = executor.execute("SELECT sum(views) AS total FROM traffic", BINDINGS)
        self.assertEqual(result.rows, [(45,)])
        self.assertEqual(result.columns, ("total",))
        self.assertEqual(result.scan_bytes_upper_bound, 100)
        with self.assertRaises(QuerySafetyError):
            executor.execute("SELECT * FROM read_parquet('/tmp/private')", BINDINGS)

    def test_scan_budget_does_not_trust_where_limit_or_join(self):
        for sql, budget in (("SELECT * FROM traffic LIMIT 0", 99), ("SELECT * FROM traffic WHERE false", 99), ("SELECT a.views FROM traffic a JOIN traffic b ON a.views=b.views", 199)):
            with self.subTest(sql=sql), self.assertRaises(QuerySafetyError) as error:
                self.executor(max_scan_bytes=budget).execute(sql, BINDINGS)
            self.assertEqual(error.exception.code, "scan_limit")

    def test_missing_binding_fails_closed(self):
        with self.assertRaises(QuerySafetyError) as error:
            self.executor().execute("SELECT * FROM traffic", {})
        self.assertEqual(error.exception.code, "view_unavailable")

    def test_result_row_limit_rejects_without_partial_success(self):
        with self.assertRaises(QuerySafetyError) as error:
            self.executor(max_rows=9).execute("SELECT * FROM traffic", BINDINGS)
        self.assertEqual(error.exception.code, "row_limit")
        self.assertEqual(len(self.executor(max_rows=10).execute("SELECT * FROM traffic", BINDINGS).rows), 10)

    def test_actual_expensive_query_times_out_and_worker_is_reaped(self):
        bindings = {"traffic": BoundView("SELECT i AS views FROM range(100000) t(i)", 1)}
        executor = self.executor(timeout_seconds=0.5)
        started = time.monotonic()
        with self.assertRaises(QuerySafetyError) as error:
            executor.execute("SELECT sum(a.views * b.views) FROM traffic a CROSS JOIN traffic b", bindings)
        self.assertEqual(error.exception.code, "time_limit")
        self.assertLess(time.monotonic() - started, 3)
        self.assertFalse(any(child.name == "lakeops-sql" for child in multiprocessing.active_children()))
        self.assertEqual(executor.execute("SELECT count(*) FROM traffic", BINDINGS).rows, [(10,)])

    def test_oversized_single_row_is_rejected(self):
        bindings = {"traffic": BoundView("SELECT repeat('x', 4096) AS text", 1)}
        with self.assertRaises(QuerySafetyError) as error:
            self.executor(max_result_bytes=1024).execute("SELECT * FROM traffic", bindings)
        self.assertEqual(error.exception.code, "result_size_limit")

    def test_binding_and_execution_errors_are_sanitized(self):
        for bindings, sql in ((BINDINGS, "SELECT secret_field FROM traffic"), ({"traffic": BoundView("SELECT * FROM '/private/secret.parquet'", 1)}, "SELECT * FROM traffic")):
            with self.subTest(sql=sql), self.assertRaises(QuerySafetyError) as error:
                self.executor().execute(sql, bindings)
            self.assertEqual(error.exception.code, "query_failure")
            self.assertNotIn("secret", str(error.exception))

    @unittest.skipUnless(Path("/proc/self/limits").exists(), "Linux worker limit inspection")
    def test_worker_enforces_os_address_space_limit(self):
        executor = self.executor(timeout_seconds=2)
        bindings = {"traffic": BoundView("SELECT i AS views FROM range(1000000) t(i)", 1)}
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(executor.execute, "SELECT sum(a.views * b.views) FROM traffic a CROSS JOIN traffic b", bindings)
            deadline = time.monotonic() + 1.5
            observed = False
            while time.monotonic() < deadline:
                for child in multiprocessing.active_children():
                    if child.name == "lakeops-sql":
                        limit_file = Path(f"/proc/{child.pid}/limits")
                        if limit_file.exists():
                            for line in limit_file.read_text().splitlines():
                                if line.startswith("Max address space") and line.split()[3:5] == [str(QueryLimits().process_memory_bytes)] * 2:
                                    observed = True
                if observed:
                    break
                time.sleep(0.01)
            self.assertTrue(observed, "worker did not establish the hard OS memory limit")
            with self.assertRaises(QuerySafetyError):
                future.result()

    def test_actual_memory_exhaustion_fails_closed(self):
        bindings = {"traffic": BoundView("SELECT i AS views FROM range(1000000) t(i)", 1)}
        with self.assertRaises(QuerySafetyError) as error:
            self.executor(memory_bytes=1_000_000).execute("SELECT DISTINCT views FROM traffic ORDER BY views", bindings)
        self.assertEqual(error.exception.code, "memory_limit")

    def test_concurrency_rejects_overlapping_request_and_recovers(self):
        executor = self.executor(timeout_seconds=1, max_concurrency=1)
        bindings = {"traffic": BoundView("SELECT i AS views FROM range(1000000) t(i)", 1)}
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(executor.execute, "SELECT sum(a.views * b.views) FROM traffic a CROSS JOIN traffic b", bindings)
            deadline = time.monotonic() + 0.5
            while executor.active_queries != 1 and time.monotonic() < deadline:
                time.sleep(0.001)
            with self.assertRaises(QuerySafetyError) as error:
                executor.execute("SELECT * FROM traffic", BINDINGS)
            self.assertEqual(error.exception.code, "concurrency_limit")
            with self.assertRaises(QuerySafetyError):
                first.result()
        self.assertEqual(executor.active_queries, 0)
        self.assertEqual(executor.execute("SELECT count(*) FROM traffic", BINDINGS).rows, [(10,)])

    def test_invalid_limits_are_rejected(self):
        for kwargs in ({"max_rows": 0}, {"memory_bytes": -1}, {"max_concurrency": True}, {"timeout_seconds": float("nan")}, {"max_scan_bytes": "100"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                QueryLimits(**kwargs)


if __name__ == "__main__":
    unittest.main()
