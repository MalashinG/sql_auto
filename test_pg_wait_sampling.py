import re
import subprocess
import threading
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
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pg_wait_sampling"


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
    if "pg_wait_sampling" not in preload:
        pytest.skip(
            "pg_wait_sampling не в shared_preload_libraries — добавьте в postgresql.conf "
            "и перезапустите сервер: shared_preload_libraries = 'pg_wait_sampling'"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_wait_sampling")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        path = next(
            (
                l
                for l in r.stdout.splitlines()
                if l.endswith("pg_wait_sampling.control")
            ),
            None,
        )
        print(f"\n  {path}")
        assert path


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = 'pg_wait_sampling'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_views_exist(self, extension):
        views = (
            "pg_wait_sampling_current",
            "pg_wait_sampling_history",
            "pg_wait_sampling_profile",
        )
        with extension.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.views WHERE table_name = ANY(%s)",
                (list(views),),
            )
            found = {r[0] for r in cur.fetchall()}
        print(f"\n  {found}")
        assert set(views) == found

    def test_collector_running(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pg_wait_sampling collector'"
            )
            count = cur.fetchone()[0]
        print(f"\n  collector-процессов: {count}")
        assert count >= 1


class TestFunctionality:

    def test_current_view_columns(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT * FROM pg_wait_sampling_current LIMIT 1")
            columns = {d.name for d in cur.description}
        print(f"\n  {columns}")
        expected = {"pid", "event_type", "event"}
        assert expected.issubset(columns), f"нет колонок: {expected - columns}"

    def test_wait_event_captured(self, extension):
        result = {}

        def sleeper():
            c = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME
            )
            with c.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                result["pid"] = cur.fetchone()[0]
                cur.execute("SELECT pg_sleep(3)")
            c.close()

        t = threading.Thread(target=sleeper)
        t.start()
        time.sleep(0.5)

        pid = result.get("pid")
        assert pid is not None, "не удалось получить pid спящего backend'а"

        found = None
        for _ in range(20):
            with extension.cursor() as cur:
                cur.execute(
                    "SELECT event_type, event FROM pg_wait_sampling_current WHERE pid = %s",
                    (pid,),
                )
                row = cur.fetchone()
            if row and row[0] == "Timeout":
                found = row
                break
            time.sleep(0.2)

        t.join()
        print(f"\n  pid={pid} event={found}")
        assert found is not None, f"wait event для pid={pid} не зафиксирован"

    def test_reset_profile(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT pg_wait_sampling_reset_profile()")
            cur.execute("SELECT count(*) FROM pg_wait_sampling_profile")
            count = cur.fetchone()[0]
        print(f"\n  строк в profile после reset: {count}")
        assert count >= 0
