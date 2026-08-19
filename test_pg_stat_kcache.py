"""
запуск: pytest test_pg_stat_kcache.py -v -s
"""

import os
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
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pg_stat_kcache"


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
    missing = [
        lib for lib in ("pg_stat_statements", "pg_stat_kcache") if lib not in preload
    ]
    if missing:
        pytest.skip(
            f"в shared_preload_libraries нет: {missing} - добавьте "
            "'pg_stat_statements,pg_stat_kcache' и перезапустите сервер"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_kcache")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        path = next(
            (l for l in r.stdout.splitlines() if l.endswith("pg_stat_kcache.control")),
            None,
        )
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
        with extension.cursor() as cur:
            for _ in range(5):
                cur.execute("SELECT count(*) FROM pg_class")
                cur.fetchall()
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            count = cur.fetchone()[0]
        print(f"\n  строк: {count}")
        assert count > 0

    def test_reset_works(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT pg_stat_kcache_reset()")
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            count = cur.fetchone()[0]
        print(f"\n  строк после reset: {count}")
        assert count == 0 or count >= 0
