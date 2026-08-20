"""
запуск: pytest test_system_stats.py -v -s
"""

import re
import subprocess
import time

import psycopg2
import pytest

PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_DBNAME = "postgres"


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def _detect():
    try:
        out = subprocess.check_output(
            ["rpm", "-qa", "--queryformat", "%{NAME}\t%{VERSION}\n"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pytest.exit("rpm не найден")

    for line in out.splitlines():
        m = re.match(r"(postgresql(\d+)(st)?-server)\t(\d+\.\d+)", line)
        if not m:
            continue
        pkg_name, major, suffix, pkg_ver = (
            m.group(1),
            m.group(2),
            m.group(3) or "",
            m.group(4),
        )
        for svc in [f"postgresql{major}", f"postgresql{major}{suffix}"]:
            if run(["systemctl", "is-active", "--quiet", svc]).returncode == 0:
                return {
                    "pkg_name": pkg_name,
                    "pkg_ver": pkg_ver,
                    "major": major,
                    "service": svc,
                }

    pytest.exit("активный postgresql*-server не найден")


INFO = _detect()
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-system_stats"


def wait_pg(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                dbname=PG_DBNAME,
                connect_timeout=2,
            )
            c.close()
            return True
        except psycopg2.OperationalError:
            time.sleep(1)
    return False


@pytest.fixture(scope="session", autouse=True)
def ensure_service():
    r = run(["systemctl", "is-active", "--quiet", INFO["service"]])
    assert r.returncode == 0, f"{INFO['service']} не запущен"
    assert wait_pg(), "PostgreSQL не отвечает"


@pytest.fixture(scope="session")
def conn(ensure_service):
    c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
    c.autocommit = True
    yield c
    c.close()


@pytest.fixture(scope="session", autouse=True)
def preload_ready(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW shared_preload_libraries")
        preload = cur.fetchone()[0]
    if "system_stats" not in preload:
        pytest.skip(
            "system_stats не в shared_preload_libraries — добавьте в postgresql.conf "
            "и перезапустите сервер: shared_preload_libraries = 'system_stats'"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS system_stats")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        path = next(
            (l for l in r.stdout.splitlines() if l.endswith("system_stats.control")),
            None,
        )
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
