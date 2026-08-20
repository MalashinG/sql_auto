import glob
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
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pgauditlogtofile"


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
    missing = [lib for lib in ("pgaudit", "pgauditlogtofile") if lib not in preload]
    if missing:
        pytest.skip(
            f"в shared_preload_libraries нет: {missing} — добавьте "
            "'pgaudit,pgauditlogtofile' (именно в этом порядке) и перезапустите сервер"
        )


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile")
    yield conn


def _find_guc(cur, *substrings):
    """Ищет GUC по подстрокам в имени, не завязываясь на точное имя параметра."""
    cur.execute("SELECT name, setting FROM pg_settings WHERE name ILIKE '%pgaudit%'")
    rows = cur.fetchall()
    for name, setting in rows:
        low = name.lower()
        if all(s in low for s in substrings):
            return name, setting
    return None


def _resolve_log_dir(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW data_directory")
        pgdata = cur.fetchone()[0]
        dir_guc = _find_guc(cur, "director")
    if not dir_guc:
        return None, pgdata
    _, dir_value = dir_guc
    log_dir = (
        dir_value if dir_value.startswith("/") else os.path.join(pgdata, dir_value)
    )
    return log_dir, pgdata


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
                if l.endswith("pgauditlogtofile.control")
            ),
            None,
        )
        print(f"\n  {path}")
        assert path


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = 'pgauditlogtofile'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None

    def test_directory_guc_found(self, extension):
        log_dir, pgdata = _resolve_log_dir(extension)
        print(f"\n  data_directory={pgdata}")
        print(f"  log_dir={log_dir}")
        assert (
            log_dir is not None
        ), "не найден GUC директории аудит-лога (искали *pgaudit*...director*)"


class TestFunctionality:

    def test_audit_line_in_file(self, extension):
        log_dir, _ = _resolve_log_dir(extension)
        assert log_dir is not None

        marker = f"sql_auto_ltf_{os.getpid()}_{int(time.time())}"

        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker} */")
        c.close()

        found = False
        checked_files = []
        for _ in range(20):
            try:
                files = sorted(
                    glob.glob(os.path.join(log_dir, "*")),
                    key=os.path.getmtime,
                    reverse=True,
                )
            except OSError:
                files = []
            for f in files[:5]:
                if not os.path.isfile(f):
                    continue
                checked_files.append(f)
                try:
                    with open(f, encoding="utf-8", errors="ignore") as fh:
                        content = fh.read()
                except OSError:
                    continue
                if marker in content:
                    found = True
                    break
            if found:
                break
            time.sleep(0.5)

        print(f"\n  marker={marker}")
        print(f"  проверенные файлы: {checked_files[:5]}")
        assert found, f"метка {marker} не найдена в файлах каталога {log_dir}"

    def test_not_polluting_main_journal(self, extension):
        """Раз pgauditlogtofile настроен — AUDIT-строки НЕ должны идти в journalctl."""
        marker = f"sql_auto_ltf_isolation_{os.getpid()}_{int(time.time())}"

        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker} */")
        c.close()

        time.sleep(1)
        r = run(["journalctl", "-u", INFO["service"], "--no-pager", "-n", "200"])
        in_journal = marker in r.stdout

        print(f"\n  marker в journalctl: {in_journal}")
        assert (
            not in_journal
        ), "AUDIT-строка попала в journalctl, хотя должна была уйти в отдельный файл"
