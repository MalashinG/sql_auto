"""
Версия определяется автоматически из установленного rpm пакета.
Запуск: pytest test.py -v -s
"""

import os

import pytest

from pg_common import INFO, PG_DBNAME, PG_HOST, PG_PORT, PG_USER, new_conn, pg_bin, run


def psql(*args, dbname=PG_DBNAME):
    return run(
        [
            pg_bin("psql"),
            "-U",
            PG_USER,
            "-h",
            PG_HOST,
            "-p",
            str(PG_PORT),
            "-d",
            dbname,
            "-v",
            "ON_ERROR_STOP=1",
            *args,
        ]
    )


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

    def test_datadir_initialized_by_this_version(self, conn):
        # PG_VERSION в корне datadir пишет initdb - это ровно мажорная версия,
        # которой каталог был создан. pg_read_file доступен суперпользователю,
        # путь относительный - от data_directory, поэтому sudo не нужен.
        with conn.cursor() as cur:
            cur.execute("SHOW data_directory")
            data_dir = cur.fetchone()[0]
            cur.execute("SELECT pg_read_file('PG_VERSION')")
            datadir_ver = cur.fetchone()[0].strip()
        print(f"\n  data_directory = {data_dir}")
        print(f"  PG_VERSION = {datadir_ver}, ожидали {INFO['major']}")
        assert (
            datadir_ver == INFO["major"]
        ), "datadir создан другой версией - возможно остался от предыдущей итерации"


class TestService:

    def test_service_active(self):
        status = run(["systemctl", "is-active", INFO["service"]]).stdout.strip()
        print(f"\n   {INFO['service']}: {status}")
        assert status == "active"

    def test_service_not_failed(self):
        # is-failed возвращает 0, если юнит в состоянии failed
        r = run(["systemctl", "is-failed", "--quiet", INFO["service"]])
        print(f"\n  is-failed returncode = {r.returncode}")
        assert r.returncode != 0, f"{INFO['service']} в состоянии failed"

    def test_service_no_auto_restarts(self):
        # NRestarts - сколько раз systemd сам перезапускал упавший сервис
        r = run(["systemctl", "show", "-p", "NRestarts", "--value", INFO["service"]])
        print(f"\n  NRestarts = {r.stdout.strip()}")
        assert r.stdout.strip() in ("", "0"), "сервис падал и перезапускался systemd"

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

        fresh = new_conn()
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

        r = psql(
            "-t",
            "-A",
            "-c",
            "SELECT name, value FROM _autotest WHERE name = 'psql_check'",
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

        r = psql("-t", "-A", "-c", "SELECT COUNT(*) FROM _autotest")
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

    RESTORE_DB = "_autotest_restore"

    def test_dump_and_restore_roundtrip(self, tmp_table, tmp_path):
        """pg_dump таблицы -> восстановление в новую БД -> данные совпадают."""
        rows = [("alpha", 1), ("beta", 2), ("gamma", 3)]
        with tmp_table.cursor() as cur:
            cur.executemany(
                "INSERT INTO _autotest (name, value) VALUES (%s, %s)", rows
            )

        dump_path = tmp_path / "autotest_dump.sql"
        # -t: только тестовая таблица - иначе в дамп попадут расширения
        # из других тестов (pg_cron, например, нельзя создать в другой БД)
        r = run(
            [
                pg_bin("pg_dump"),
                "-U",
                PG_USER,
                "-h",
                PG_HOST,
                "-p",
                str(PG_PORT),
                "-t",
                "_autotest",
                "-f",
                str(dump_path),
                PG_DBNAME,
            ]
        )
        print(f"\n  pg_dump returncode: {r.returncode}")
        assert r.returncode == 0, f"pg_dump упал: {r.stderr}"
        assert dump_path.stat().st_size > 0

        with tmp_table.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {self.RESTORE_DB}")
            cur.execute(f"CREATE DATABASE {self.RESTORE_DB}")
        try:
            r = psql("-q", "-f", str(dump_path), dbname=self.RESTORE_DB)
            print(f"  restore returncode: {r.returncode}")
            assert r.returncode == 0, f"восстановление упало: {r.stderr}"

            restored = new_conn(dbname=self.RESTORE_DB)
            try:
                with restored.cursor() as cur:
                    cur.execute("SELECT name, value FROM _autotest ORDER BY id")
                    restored_rows = cur.fetchall()
            finally:
                restored.close()
            print(f"  восстановлено: {restored_rows}")
            assert restored_rows == rows
        finally:
            with tmp_table.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {self.RESTORE_DB}")
