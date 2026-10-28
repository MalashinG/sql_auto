"""
запуск: pytest test_pgaudit.py -v -s
"""

import os
import time

import pytest

from pg_common import (
    addon_pkg,
    find_in_pkg,
    journal_has,
    journal_since,
    new_conn,
    preload_libs,
    require_preload,
    run,
)

ADDON_PKG = addon_pkg("pgaudit")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "pgaudit")
    if "pgauditlogtofile" in preload_libs(conn):
        pytest.skip(
            "pgauditlogtofile тоже загружен - он перехватывает AUDIT-строки и "
            "уводит их в отдельный файл, поэтому их больше нет в journalctl. "
            "Это ожидаемое поведение, не баг. Для проверки в такой конфигурации "
            "используйте test_pgauditlogtofile.py (там метка ищется в файле). "
            "Чтобы прогнать этот файл - запускайте только 'pgaudit' без 'pgauditlogtofile'."
        )
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pgaudit.control")
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
        ), "GUC pgaudit.log не зарегистрирован - библиотека не загрузилась"


class TestFunctionality:

    def test_audit_line_written(self, extension):
        marker = f"sql_auto_pgaudit_{os.getpid()}_{int(time.time())}"
        since = journal_since()

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker} */")
        c.close()

        found = journal_has(marker, since, "AUDIT:")
        print(f"\n  marker={marker} найден={found}")
        assert (
            found
        ), f"AUDIT-запись с меткой {marker} не найдена в journalctl с {since}"

    def test_ddl_class_logged(self, extension):
        marker = f"sql_auto_pgaudit_ddl_{os.getpid()}_{int(time.time())}"
        table = f"_pgaudit_test_{os.getpid()}"
        since = journal_since()

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'ddl'")
            cur.execute(f"CREATE TABLE {table} (id int) /* {marker} */")
            cur.execute(f"DROP TABLE {table}")
        c.close()

        # AUDIT, класс DDL и метка должны быть в одной строке, а не где-то в журнале
        found = journal_has(marker, since, "AUDIT:", "DDL")
        print(f"\n  marker={marker} найден={found}")
        assert found, f"AUDIT DDL-запись с меткой {marker} не найдена"
