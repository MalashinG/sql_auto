"""
Версия определяется автоматически из установленного rpm пакета.
Запуск: pytest test.py -v -s
"""

import os
import re
import subprocess
import time

import psycopg2
import pytest


def _detect() -> dict:
    try:
        out = subprocess.check_output(
            ["rpm", "-qa", "--queryformat", "%{NAME}\t%{VERSION}\n"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pytest.exit(
            "rpm не найден - тест рассчитан на RPM-based дистрибутив (ROSA Linux)"
        )

    for line in out.splitlines():
        m = re.match(r"(postgresql(\d+)(st)?-server)\t(\d+\.\d+)", line)
        if m:
            pkg_name, major, suffix, pkg_ver = (
                m.group(1),
                m.group(2),
                m.group(3) or "",
                m.group(4),
            )
            for svc in [f"postgresql{major}", f"postgresql{major}{suffix}"]:
                r = subprocess.run(
                    ["systemctl", "is-active", "--quiet", svc], capture_output=True
                )
                if r.returncode == 0:
                    return {
                        "pkg_name": pkg_name,
                        "pkg_ver": pkg_ver,
                        "major": major,
                        "service": svc,
                        "bin_dir": f"/usr/libexec/postgresql{major}",
                    }

    pytest.exit(
        "Пакет postgresql*-server не найден.\n"
        "Установите пакет, например: dnf install postgresql(версия)-server"
    )


INFO = _detect()

PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_DBNAME = "postgres"


def pg_bin(name: str) -> str:
    for path in [os.path.join(INFO["bin_dir"], name), f"/usr/bin/{name}"]:
        if os.path.exists(path):
            return path
    return name


def run(cmd: list, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def wait_for_postgres(timeout: int = 30) -> bool:
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
    assert (
        r.returncode == 0
    ), f"Сервис {INFO['service']} не запущен.\nЗапустите: sudo systemctl start {INFO['service']}"
    assert wait_for_postgres(), "PostgreSQL не принимает соединения"


@pytest.fixture(scope="session")
def conn():
    c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
    c.autocommit = True
    yield c
    c.close()


@pytest.fixture()
def tmp_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _autotest")
        cur.execute("""
            CREATE TABLE _autotest (
                id    SERIAL PRIMARY KEY,
                name  TEXT    NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _autotest")


class TestPackage:

    def test_server_package_installed(self):
        r = run(["rpm", "-q", INFO["pkg_name"]])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0, f"Пакет {INFO['pkg_name']} не найден"

    def test_postgres_binary_exists(self):
        path = pg_bin("postgres")
        print(f"\n  {path}")
        assert os.path.exists(path), f"Не найден: {path}"

    def test_psql_binary_exists(self):
        path = pg_bin("psql")
        print(f"\n  {path}")
        assert os.path.exists(path), f"Не найден: {path}"


class TestVersion:

    def test_server_version_matches_package(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            ver_str = cur.fetchone()[0]
        print(f"\n  Ожидаем : PostgreSQL {INFO['pkg_ver']}")
        print(f"  Получили: {ver_str}")
        assert f"PostgreSQL {INFO['pkg_ver']}" in ver_str

    def test_psql_version_matches_package(self):
        r = run([pg_bin("psql"), "--version"])
        print(f"\n   Ожидаем : {INFO['pkg_ver']}")
        print(f"   Получили: {r.stdout.strip()}")
        assert r.returncode == 0
        assert INFO["pkg_ver"] in r.stdout

    def test_server_version_num_matches_major(self, conn):
        with conn.cursor() as cur:
            cur.execute("SHOW server_version_num")
            num = int(cur.fetchone()[0])
        major_expected = int(INFO["major"])
        major_actual = num // 10000
        print(f"\n  server_version_num = {num}, major = {major_actual}")
        assert (
            major_actual == major_expected
        ), f"Ожидали мажорную версию {major_expected}, а получили {major_actual}"

    def test_datadir_initialized_by_this_version(self):
        c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
        try:
            with c.cursor() as cur:
                cur.execute("SHOW data_directory")
                data_dir = cur.fetchone()[0]
        finally:
            c.close()

        r = run(["sudo", "-n", "-u", "postgres", "pg_controldata", data_dir])
        print(f"\n  data_directory = {data_dir}")
        print("\n".join(r.stdout.splitlines()[:5]))
        if r.returncode != 0:
            print(f"  stderr: {r.stderr.strip()}")

        if "a password is required" in r.stderr.lower() or "sudo:" in r.stderr.lower():
            pytest.skip(
                "sudo -u postgres без пароля недоступен. Добавьте в sudoers: "
                "'<user> ALL=(postgres) NOPASSWD: /usr/bin/pg_controldata'"
            )

        assert r.returncode == 0, f"pg_controldata не смог прочитать {data_dir}"
        assert (
            INFO["pkg_ver"].split(".")[0] in r.stdout
            or "catalog version" in r.stdout.lower()
        ), "pg_controldata не подтверждает версию - возможно datadir от предыдущей итерации"


class TestService:

    def test_service_active(self):
        status = run(["systemctl", "is-active", INFO["service"]]).stdout.strip()
        print(f"\n   {INFO['service']}: {status}")
        assert status == "active"

    def test_service_no_failures(self):
        status = run(["systemctl", "status", INFO["service"]])
        print("\n" + "\n".join(status.stdout.splitlines()[:5]))
        assert "failed" not in status.stdout.lower()

    def test_port_listening(self):
        result = run(["ss", "-tlnp", f"sport = :{PG_PORT}"])
        print(f"\n{result.stdout.strip()}")
        assert str(PG_PORT) in result.stdout


class TestSQL:

    def test_connect(self, conn):
        print(f"\n   {PG_HOST}:{PG_PORT} / {PG_DBNAME}")
        assert not conn.closed

    def test_select_one(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()[0]
        print(f"\n  SELECT 1 = {result}")
        assert result == 1

    def test_create_table(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = '_autotest'
                )
            """)
            exists = cur.fetchone()[0]
        print(f"\n  _autotest существует: {exists}")
        assert exists is True

    def test_insert_and_select(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute(
                "INSERT INTO _autotest (name, value) VALUES (%s, %s)", ("hello", 42)
            )
            cur.execute("SELECT name, value FROM _autotest WHERE name = 'hello'")
            row = cur.fetchone()
        print(f"\n INSERT ('hello', 42)")
        print(f" SELECT вернул: {row}")
        assert row == ("hello", 42)

    def test_socket_path_correct(self):
        result = run(
            [pg_bin("psql"), "-U", "postgres", "-c", "SHOW unix_socket_directories;"]
        )
        print(f"\n{result.stdout.strip()}")
        assert "/var/run/postgresql" in result.stdout

    def test_update(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('upd', 1)")
            cur.execute("UPDATE _autotest SET value = 99 WHERE name = 'upd'")
            cur.execute("SELECT value FROM _autotest WHERE name = 'upd'")
            result = cur.fetchone()[0]
        print(f"\n  UPDATE value = {result}")
        assert result == 99

    def test_delete(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('del', 0)")
            cur.execute("DELETE FROM _autotest WHERE name = 'del'")
            cur.execute("SELECT COUNT(*) FROM _autotest")
            count = cur.fetchone()[0]
        print(f"\n COUNT(*) после DELETE: {count}")
        assert count == 0


class TestIndependentVerification:

    def test_insert_visible_from_new_connection(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute(
                "INSERT INTO _autotest (name, value) VALUES (%s, %s)",
                ("independent_check", 777),
            )

        fresh = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME
        )
        try:
            with fresh.cursor() as cur:
                cur.execute(
                    "SELECT name, value FROM _autotest WHERE name = 'independent_check'"
                )
                row = cur.fetchone()
        finally:
            fresh.close()

        print(f"\n  Видно из нового соединения: {row}")
        assert row == ("independent_check", 777)

    def test_insert_visible_via_psql_cli(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute(
                "INSERT INTO _autotest (name, value) VALUES (%s, %s)",
                ("psql_check", 555),
            )

        r = run(
            [
                pg_bin("psql"),
                "-U",
                PG_USER,
                "-h",
                PG_HOST,
                "-d",
                PG_DBNAME,
                "-t",
                "-A",
                "-c",
                "SELECT name, value FROM _autotest WHERE name = 'psql_check'",
            ]
        )
        output = r.stdout.strip()
        print(f"\n  psql видит: '{output}'")
        assert r.returncode == 0
        assert output == "psql_check|555"

    def test_row_count_matches_across_clients(self, tmp_table):
        with tmp_table.cursor() as cur:
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('a', 1)")
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('b', 2)")
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('c', 3)")
            cur.execute("SELECT COUNT(*) FROM _autotest")
            count_psycopg2 = cur.fetchone()[0]

        r = run(
            [
                pg_bin("psql"),
                "-U",
                PG_USER,
                "-h",
                PG_HOST,
                "-d",
                PG_DBNAME,
                "-t",
                "-A",
                "-c",
                "SELECT COUNT(*) FROM _autotest",
            ]
        )
        count_psql = int(r.stdout.strip())

        print(f"\n  psycopg2 COUNT(*) = {count_psycopg2}")
        print(f"  psql     COUNT(*) = {count_psql}")
        assert count_psycopg2 == count_psql == 3


class TestExtensions:

    def test_pg_profile_extension_loads(self, conn):
        r = run(["rpm", "-qa", "*pg_profile*"])
        if not r.stdout.strip():
            pytest.skip("pg_profile не установлен в этом прогоне")
        print(f"\n  найден пакет: {r.stdout.strip()}")

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_available_extensions WHERE name = 'dblink'")
            dblink_available = cur.fetchone() is not None

        if not dblink_available:
            pytest.fail(
                "dblink недоступен - обязательная зависимость pg_profile. "
                "Установите postgresqlXX-contrib."
            )

        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS dblink CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_profile CASCADE")
            cur.execute(
                "SELECT extversion FROM pg_extension WHERE extname = 'pg_profile'"
            )
            row = cur.fetchone()
        print(f"  pg_profile extversion: {row}")
        assert row is not None


class TestDumpRestore:

    DUMP_PATH = "/tmp/_autotest_dump.sql"

    def test_pg_dump_works(self, tmp_table):
        r = run(
            [
                pg_bin("pg_dump"),
                "-U",
                PG_USER,
                "-h",
                PG_HOST,
                PG_DBNAME,
                "-f",
                self.DUMP_PATH,
            ]
        )
        print(f"\n  returncode: {r.returncode}")
        if r.stderr:
            print(f"  stderr: {r.stderr[:300]}")
        assert r.returncode == 0, f"pg_dump упал: {r.stderr}"
        assert os.path.exists(self.DUMP_PATH)
        assert os.path.getsize(self.DUMP_PATH) > 0

    def test_dump_contains_test_table(self, tmp_table):
        assert os.path.exists(self.DUMP_PATH), "Запустите test_pg_dump_works первым"
        with open(self.DUMP_PATH, encoding="utf-8", errors="ignore") as f:
            content = f.read()
        print(f"\n  размер дампа: {len(content)} байт")
        assert "_autotest" in content
