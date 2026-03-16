"""
Версия определяется автоматически из установленного rpm пакета.

запуск:
    pytest test.py -v      # обычный вывод
    pytest test.py -v -s   # подробный вывод
"""

import os
import re
import subprocess
import time

import psycopg2
import pytest


# ---- Определение установленного пакета

def _detect() -> dict:
    try:
        out = subprocess.check_output(
            ["rpm", "-qa", "--queryformat", "%{NAME}\t%{VERSION}\n"],
            text=True, stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pytest.exit("rpm не найден — тест рассчитан на RPM-based дистрибутив (ROSA Linux)")

    for line in out.splitlines():
        m = re.match(r"(postgresql(\d+)(st)?-server)\t(\d+\.\d+)", line)
        if m:
            pkg_name, major, suffix, pkg_ver = (
                m.group(1), m.group(2), m.group(3) or "", m.group(4)
            )
            return {
                "pkg_name": pkg_name,
                "pkg_ver":  pkg_ver,
                "major":    major,
                "service":  f"postgresql{major}{suffix}",
                "bin_dir":  f"/usr/lib/postgresql{major}{suffix}/bin",
            }

    pytest.exit(
        "Пакет postgresql*-server не найден.\n"
        "Установите пакет, например: dnf install postgresql(версия)-server"
    )


INFO = _detect()

PG_HOST   = "localhost"
PG_PORT   = 5432
PG_USER   = "postgres"
PG_DBNAME = "postgres"


# ------- функции

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
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, dbname=PG_DBNAME,
                connect_timeout=2,
            )
            c.close()
            return True
        except psycopg2.OperationalError:
            time.sleep(1)
    return False


# ----- фикстуры

@pytest.fixture(scope="session", autouse=True)
def ensure_service():
    r = run(["systemctl", "is-active", "--quiet", INFO["service"]])
    assert r.returncode == 0, (
        f"Сервис {INFO['service']} не запущен.\n"
        f"Запустите: sudo systemctl start {INFO['service']}"
    )
    assert wait_for_postgres(), "PostgreSQL не принимает соединения"


@pytest.fixture(scope="session")
def conn():
    c = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, dbname=PG_DBNAME,
    )
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


# ---- 1 Пакет

class TestPackage:

    def test_server_package_installed(self):
        """RPM-пакет postgresql*-server установлен."""
        r = run(["rpm", "-q", INFO["pkg_name"]])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0, f"Пакет {INFO['pkg_name']} не найден"

    def test_postgres_binary_exists(self):
        """Исполняемый файл postgres присутствует на диске."""
        path = pg_bin("postgres")
        print(f"\n  {path}")
        assert os.path.exists(path), f"Не найден: {path}"

    def test_psql_binary_exists(self):
        """Исполняемый файл psql присутствует на диске."""
        path = pg_bin("psql")
        print(f"\n  {path}")
        assert os.path.exists(path), f"Не найден: {path}"


# ------ 2 Версия

class TestVersion:

    def test_server_version_matches_package(self, conn):
        """SELECT version() совпадает с версией rpm-пакета."""
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            ver_str = cur.fetchone()[0]
        print(f"\n  Ожидаем : PostgreSQL {INFO['pkg_ver']}")
        print(f"  Получили: {ver_str}")
        assert f"PostgreSQL {INFO['pkg_ver']}" in ver_str

    def test_psql_version_matches_package(self):
        """psql --version совпадает с версией rpm-пакета."""
        r = run([pg_bin("psql"), "--version"])
        print(f"\n   Ожидаем : {INFO['pkg_ver']}")
        print(f"   Получили: {r.stdout.strip()}")
        assert r.returncode == 0
        assert INFO["pkg_ver"] in r.stdout


# ------ 3 Сервис

class TestService:

    def test_service_active(self):
        """Сервис находится в состоянии active."""
        status = run(["systemctl", "is-active", INFO["service"]])
        status = status.stdout.strip()
        print(f"\n   {INFO['service']}: {status}")
        assert status == "active"

    def test_service_no_failures(self):
        """systemctl status не содержит 'failed'."""
        status = run(["systemctl", "status", INFO["service"]])
        print(f"\n" + "\n".join(status.stdout.splitlines()[:5]))
        assert "failed" not in status.stdout.lower()

    def test_port_listening(self):
        """PostgreSQL слушает порт 5432."""
        result = run(["ss", "-tlnp", f"sport = :{PG_PORT}"])
        print(f"\n{result.stdout.strip()}")
        assert str(PG_PORT) in result.stdout


# ----- 4. SQL

class TestSQL:

    def test_connect(self, conn):
        """Подключение к PostgreSQL установлено."""
        print(f"\n   {PG_HOST}:{PG_PORT} / {PG_DBNAME}")
        assert not conn.closed

    def test_select_one(self, conn):
        """SELECT 1 возвращает 1."""
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()[0]
        print(f"\n  SELECT 1 = {result}")
        assert result == 1

    def test_create_table(self, tmp_table):
        """CREATE TABLE — таблица появляется в information_schema."""
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
        """INSERT + SELECT возвращает вставленные данные."""
        with tmp_table.cursor() as cur:
            cur.execute(
                "INSERT INTO _autotest (name, value) VALUES (%s, %s)",
                ("hello", 42),
            )
            cur.execute("SELECT name, value FROM _autotest WHERE name = 'hello'")
            row = cur.fetchone()
        print(f"\n INSERT ('hello', 42)")
        print(f" SELECT вернул: {row}")
        assert row == ("hello", 42)

    def test_update(self, tmp_table):
        """UPDATE изменяет значение записи."""
        with tmp_table.cursor() as cur:
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('upd', 1)")
            cur.execute("UPDATE _autotest SET value = 99 WHERE name = 'upd'")
            cur.execute("SELECT value FROM _autotest WHERE name = 'upd'")
            result = cur.fetchone()[0]
        print(f"\n  UPDATE value = {result}")
        assert result == 99

    def test_delete(self, tmp_table):
        """DELETE удаляет запись."""
        with tmp_table.cursor() as cur:
            cur.execute("INSERT INTO _autotest (name, value) VALUES ('del', 0)")
            cur.execute("DELETE FROM _autotest WHERE name = 'del'")
            cur.execute("SELECT COUNT(*) FROM _autotest")
            count = cur.fetchone()[0]
        print(f"\n COUNT(*) после DELETE: {count}")
        assert count == 0