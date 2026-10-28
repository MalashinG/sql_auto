"""
запуск: pytest test_system_stats.py -v -s
"""

import pytest

from pg_common import addon_pkg, find_in_pkg, require_preload, run

ADDON_PKG = addon_pkg("system_stats")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "system_stats")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS system_stats")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "system_stats.control")
        print(f"\n  {path}")
        assert path


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = 'system_stats'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_monitor_role_exists(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = 'monitor_system_stats'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None


class TestFunctionality:

    def test_cpu_info(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_sys_cpu_info()")
            rows = cur.fetchall()
            columns = {d.name for d in cur.description}
        print(f"\n  columns={columns}")
        print(f"  rows={len(rows)}")
        assert rows

    def test_memory_info(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_sys_memory_info()")
            row = cur.fetchone()
            columns = {d.name for d in cur.description}
        print(f"\n  {row}")
        expected = {"total_memory", "used_memory", "free_memory"}
        assert expected.issubset(columns), f"нет колонок: {expected - columns}"
        assert row is not None

    def test_disk_info(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_sys_disk_info()")
            rows = cur.fetchall()
        print(f"\n  rows={len(rows)}")
        assert rows

    def test_cpu_memory_by_process(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_sys_cpu_memory_by_process() LIMIT 5")
            rows = cur.fetchall()
            columns = {d.name for d in cur.description}
        print(f"\n  columns={columns}")
        print(f"  rows={len(rows)}")
        expected = {"pid", "name", "cpu_usage", "memory_usage"}
        assert expected.issubset(columns), f"нет колонок: {expected - columns}"
        assert rows
