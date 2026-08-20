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
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pgaudit"


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
    if "pgaudit" not in preload:
        pytest.skip(
            "pgaudit не в shared_preload_libraries — добавьте в postgresql.conf "
            "и перезапустите сервер: shared_preload_libraries = 'pgaudit'"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        path = next(
            (l for l in r.stdout.splitlines() if l.endswith("pgaudit.control")), None
        )
        print(f"\n  {path}")
        assert path


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pgaudit'")
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_log_guc_registered(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT name, setting FROM pg_settings WHERE name = 'pgaudit.log'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert (
            row is not None
        ), "GUC pgaudit.log не зарегистрирован — библиотека не загрузилась"


class TestFunctionality:

    def test_audit_line_written(self, extension):
        marker = f"sql_auto_pgaudit_{os.getpid()}_{int(time.time())}"

        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker} */")
        c.close()

        found = False
        for _ in range(10):
            r = run(["journalctl", "-u", INFO["service"], "--no-pager", "-n", "500"])
            if "AUDIT:" in r.stdout and marker in r.stdout:
                found = True
                break
            time.sleep(0.5)

        print(f"\n  marker={marker} найден={found}")
        assert (
            found
        ), f"AUDIT-запись с меткой {marker} не найдена в journalctl -u {INFO['service']}"

    def test_ddl_class_logged(self, extension):
        marker = f"sql_auto_pgaudit_ddl_{os.getpid()}_{int(time.time())}"

        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'ddl'")
            cur.execute(
                f"CREATE TABLE _pgaudit_test_{os.getpid()} (id int) /* {marker} */"
            )
            cur.execute(f"DROP TABLE _pgaudit_test_{os.getpid()}")
        c.close()

        found = False
        for _ in range(10):
            r = run(["journalctl", "-u", INFO["service"], "--no-pager", "-n", "500"])
            if "AUDIT:" in r.stdout and "DDL" in r.stdout and marker in r.stdout:
                found = True
                break
            time.sleep(0.5)

        print(f"\n  marker={marker} найден={found}")
        assert found, f"AUDIT DDL-запись с меткой {marker} не найдена"
