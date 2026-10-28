"""
запуск: pytest test_pgauditlogtofile.py -v -s
"""

import os
import time

import pytest

from pg_common import (
    INFO,
    addon_pkg,
    audit_log_dir,
    find_in_pkg,
    find_marker_in_dir,
    journal_since,
    new_conn,
    require_preload,
    run,
)

ADDON_PKG = addon_pkg("pgauditlogtofile")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "pgaudit", "pgauditlogtofile")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile")
    yield conn


def audit_select(marker):
    c = new_conn()
    with c.cursor() as cur:
        cur.execute("SET pgaudit.log = 'all'")
        cur.execute(f"SELECT 1 /* {marker} */")
    c.close()


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pgauditlogtofile.control")
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
        log_dir = audit_log_dir(extension)
        print(f"\n  log_dir={log_dir}")
        assert (
            log_dir is not None
        ), "не найден GUC директории аудит-лога (искали *pgaudit*...director*)"


class TestFunctionality:

    def test_audit_line_in_file(self, extension):
        log_dir = audit_log_dir(extension)
        assert log_dir is not None

        marker = f"sql_auto_ltf_{os.getpid()}_{int(time.time())}"
        audit_select(marker)

        path = find_marker_in_dir(log_dir, marker)
        print(f"\n  marker={marker} найден в: {path}")
        assert path, f"метка {marker} не найдена в файлах каталога {log_dir}"

    def test_not_polluting_main_journal(self, extension):
        """Раз pgauditlogtofile настроен - AUDIT-строки НЕ должны идти в journalctl."""
        log_dir = audit_log_dir(extension)
        assert log_dir is not None

        marker = f"sql_auto_ltf_isolation_{os.getpid()}_{int(time.time())}"
        since = journal_since()
        audit_select(marker)

        # сначала убеждаемся, что запись вообще дошла до файла - иначе
        # "нет в журнале" ничего не доказывает (могла просто не записаться)
        assert find_marker_in_dir(log_dir, marker), "метка не записалась даже в файл"

        r = run(["journalctl", "-u", INFO["service"], "--no-pager", "--since", since])
        in_journal = marker in r.stdout

        print(f"\n  marker в journalctl: {in_journal}")
        assert (
            not in_journal
        ), "AUDIT-строка попала в journalctl, хотя должна была уйти в отдельный файл"
