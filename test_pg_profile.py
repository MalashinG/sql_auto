"""
запуск: pytest test_pg_profile.py -v -s
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

REPORTS_DIR = os.environ.get("PG_PROFILE_REPORTS_DIR", "./pg_profile_reports")


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
                    "bin_dir": f"/usr/libexec/postgresql{major}",
                }

    pytest.exit("активный postgresql*-server не найден")


INFO = _detect()
ADDON_PKG = INFO["pkg_name"][: -len("-server")] + "-pg_profile"
DEVEL_PKG = INFO["pkg_name"][: -len("-server")] + "-devel"


def pg_bin(name):
    for path in [os.path.join(INFO["bin_dir"], name), f"/usr/bin/{name}"]:
        if os.path.exists(path):
            return path
    return name


def save_report(filename, html):
    os.makedirs(REPORTS_DIR, exist_ok=True)
    path = os.path.join(REPORTS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


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


def find_in_pkg(pkg, suffix):
    r = run(["rpm", "-ql", pkg])
    return next((l for l in r.stdout.splitlines() if l.endswith(suffix)), None)


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


@pytest.fixture(scope="session")
def extension(conn):
    with conn.cursor() as cur:
        cur.execute("DROP EXTENSION IF EXISTS pg_profile CASCADE")
        cur.execute("DROP SCHEMA IF EXISTS profile CASCADE")
        cur.execute("CREATE EXTENSION IF NOT EXISTS dblink")
        cur.execute("CREATE SCHEMA profile")
        cur.execute("CREATE EXTENSION pg_profile SCHEMA profile")
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS profile CASCADE")


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pg_profile.control")
        print(f"\n  {path}")
        assert path and os.path.exists(path)

    def test_sql_scripts(self):
        r = run(["rpm", "-ql", ADDON_PKG])
        scripts = [
            l
            for l in r.stdout.splitlines()
            if os.path.basename(l).startswith("pg_profile--")
        ]
        print(f"\n  {len(scripts)} шт.")
        assert scripts

    def test_dblink(self):
        profile_control = find_in_pkg(ADDON_PKG, "pg_profile.control")
        ext_dir = os.path.dirname(profile_control) if profile_control else None
        path = os.path.join(ext_dir, "dblink.control") if ext_dir else None
        print(f"\n  {path}")
        assert path and os.path.exists(path), "dblink - нужен contrib"

    def test_devel_installed(self):
        r = run(["rpm", "-q", DEVEL_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_pg_config_sharedir(self):
        profile_control = find_in_pkg(ADDON_PKG, "pg_profile.control")
        expected = os.path.dirname(profile_control) if profile_control else None

        r = run([pg_bin("pg_config"), "--sharedir"])
        assert r.returncode == 0
        actual = os.path.join(r.stdout.strip(), "extension")
        print(f"\n  pg_config: {actual}")
        print(f"  ожидали: {expected}")
        assert actual == expected


class TestExtension:

    def test_create(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extname, extnamespace::regnamespace FROM pg_extension WHERE extname = 'pg_profile'"
            )
            row = cur.fetchone()
        print(f"\n  {row}")
        assert row is not None
        assert row[1] == "profile"

    def test_version_matches_rpm(self, extension):
        pkg_ver = run(["rpm", "-q", "--qf", "%{VERSION}", ADDON_PKG]).stdout.strip()
        with extension.cursor() as cur:
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'pg_profile'"
            )
            ext_ver = cur.fetchone()[0]
        print(f"\n  rpm={pkg_ver} ext={ext_ver}")
        assert ext_ver == pkg_ver

    def test_schema_exists(self, extension):
        with extension.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'profile')"
            )
            exists = cur.fetchone()[0]
        print(f"\n  {exists}")
        assert exists is True

    def test_functions_exist(self, extension):
        with extension.cursor() as cur:
            cur.execute("""
                SELECT proname FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'profile'
                  AND proname IN ('take_sample', 'get_report', 'show_samples', 'show_servers')
            """)
            found = {r[0] for r in cur.fetchall()}
        print(f"\n  {found}")
        assert {"take_sample", "get_report", "show_samples", "show_servers"}.issubset(
            found
        )

    def test_local_server(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT server_name, enabled FROM profile.show_servers()")
            servers = cur.fetchall()
        print(f"\n  {servers}")
        assert "local" in {s[0] for s in servers}


class TestFunctionality:

    def test_take_sample(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT result, elapsed FROM profile.take_sample()")
            rows = cur.fetchall()
        print(f"\n  {rows}")
        assert rows
        for result, elapsed in rows:
            assert result == "OK"

    @pytest.fixture(scope="class")
    def two_samples(self, extension):
        with extension.cursor() as cur:
            cur.execute("SELECT profile.take_sample()")
            cur.execute("SELECT profile.take_sample()")
            cur.execute("SELECT sample FROM profile.show_samples() ORDER BY sample")
            ids = [r[0] for r in cur.fetchall()]
        assert len(ids) >= 2
        return ids[0], ids[-1]

    def test_report_sections(self, extension, two_samples):
        start_id, end_id = two_samples
        with extension.cursor() as cur:
            cur.execute("SELECT profile.get_report(%s, %s)", (start_id, end_id))
            report = cur.fetchone()[0]

        path = save_report(
            f"sections_{start_id}_{end_id}_pg{INFO['major']}.html", report
        )
        print(f"\n  {path}")

        assert '"local"' in report
        assert "buildReport" in report
        assert 'id="container"' in report

    def test_full_report(self, extension, two_samples):
        start_id, end_id = two_samples
        with extension.cursor() as cur:
            cur.execute("SELECT profile.get_report(%s, %s)", (start_id, end_id))
            report = cur.fetchone()[0]

        path = save_report(f"report_{start_id}_{end_id}_pg{INFO['major']}.html", report)
        print(f"\n  {path}, {len(report) if report else 0} символов")
        assert report and len(report) > 500 and "<html" in report.lower()

    def test_cleanup(self, conn, extension):
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA profile CASCADE")
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'profile')"
            )
            exists = cur.fetchone()[0]
        print(f"\n  осталась: {exists}")
        assert exists is False
