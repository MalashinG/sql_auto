"""
запуск: pytest test_pg_cron.py -v -s
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
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pg_cron"


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
    if "pg_cron" not in preload:
        pytest.skip(
            "pg_cron не в shared_preload_libraries -"
            " добавьте в postgresql.conf "
            "и перезапустите сервер: shared_preload_libraries = 'pg_cron'"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        path = next(
            (l for l in r.stdout.splitlines() if l.endswith("pg_cron.control")), None
        )
        print(f"\n  {path}")
        assert path and os.path.exists(path)


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_cron'")
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_job_table_exists(self, extension):
        with extension.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'cron' AND table_name = 'job'
                )
            """)
            exists = cur.fetchone()[0]
        print(f"\n  {exists}")
        assert exists is True

    def test_launcher_running(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_cron launcher'"
            )
            count = cur.fetchone()[0]
        print(f"\n  launcher-процессов: {count}")
        assert count >= 1


class TestFunctionality:

    @pytest.fixture()
    def scheduled_job(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT cron.schedule('sql_auto_test_job', '5 seconds', $$SELECT 1$$)"
            )
            jobid = cur.fetchone()[0]
        yield jobid
        with extension.cursor() as cur:
            cur.execute("SELECT cron.unschedule(%s)", (jobid,))
            cur.execute("DELETE FROM cron.job_run_details WHERE jobid = %s", (jobid,))

    def test_schedule_creates_job(self, extension, scheduled_job):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT jobname, schedule, active FROM cron.job WHERE jobid = %s",
                (scheduled_job,),
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None
        assert row[2] is True

    def test_job_actually_runs(self, extension, scheduled_job):
        succeeded = False
        for _ in range(20):
            with extension.cursor() as cur:
                cur.execute(
                    "SELECT status FROM cron.job_run_details WHERE jobid = %s ORDER BY start_time DESC LIMIT 1",
                    (scheduled_job,),
                )
                row = cur.fetchone()
            if row and row[0] == "succeeded":
                succeeded = True
                break
            time.sleep(1)
        print(f"\n  succeeded={succeeded}")
        assert succeeded, "Job не выполнился за20 секунд"

    def test_unschedule_removes_job(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT cron.schedule('sql_auto_test_unschedule', '5 seconds', $$SELECT 1$$)"
            )
            jobid = cur.fetchone()[0]
            cur.execute("SELECT cron.unschedule(%s)", (jobid,))
            cur.execute("SELECT count(*) FROM cron.job WHERE jobid = %s", (jobid,))
            count = cur.fetchone()[0]
        print(f"\n  осталось записей: {count}")
        assert count == 0
