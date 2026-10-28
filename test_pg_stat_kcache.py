"""
запуск: pytest test_pg_stat_kcache.py -v -s
"""

import os

import pytest

from pg_common import addon_pkg, find_in_pkg, require_preload, run

ADDON_PKG = addon_pkg("pg_stat_kcache")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "pg_stat_statements", "pg_stat_kcache")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_kcache")
    yield conn


def kcache_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_stat_kcache()")
        return cur.fetchone()[0]


def run_distinct_queries(conn, n=10):
    """n структурно разных запросов -> n разных queryid в статистике.
    Константы и алиасы при расчёте queryid нормализуются, поэтому
    меняем число колонок: SELECT 1 / SELECT 1, 1 / ..."""
    with conn.cursor() as cur:
        for i in range(n):
            cur.execute("SELECT " + ", ".join(["1"] * (i + 1)))
            cur.fetchall()


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pg_stat_kcache.control")
        print(f"\n  {path}")
        assert path and os.path.exists(path)


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = 'pg_stat_kcache'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_pg_stat_statements_dependency(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = 'pg_stat_statements'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_reset_function_exists(self, extension):
        with extension.cursor() as cur:
            cur.execute("""
                SELECT proname FROM pg_proc
                WHERE proname = 'pg_stat_kcache_reset'
            """)
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None


class TestFunctionality:

    def test_view_returns_columns(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_stat_kcache() LIMIT 1")
            columns = {d.name for d in cur.description}
        print(f"\n  {columns}")

        # не привязываемся к конкретной версии/схеме именования (exec_*/plan_*
        # появились в 2.2+, раньше были плоские имена) - просто ищем колонку
        # с нужным смысловым суффиксом, префикс не важен
        required_suffixes = ["reads", "writes", "user_time", "system_time"]
        missing = [
            s for s in required_suffixes if not any(c.endswith(s) for c in columns)
        ]
        assert not missing, f"нет колонок с суффиксами {missing}. Есть: {columns}"

    def test_view_has_data_after_query(self, extension):
        run_distinct_queries(extension, 5)
        count = kcache_rows(extension)
        print(f"\n  строк: {count}")
        assert count > 0

    def test_reset_works(self, extension):
        run_distinct_queries(extension, 10)
        before = kcache_rows(extension)

        with extension.cursor() as cur:
            cur.execute("SELECT pg_stat_kcache_reset()")
        after = kcache_rows(extension)

        # точный ноль не требуем: сам вызов reset тоже попадает в статистику
        print(f"\n  строк до reset: {before}, после: {after}")
        assert before >= 10, "статистика не накопилась - проверять reset не на чем"
        assert after < before, "pg_stat_kcache_reset() не очистил статистику"
