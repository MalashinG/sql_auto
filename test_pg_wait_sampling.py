"""
запуск: pytest test_pg_wait_sampling.py -v -s
"""

import time

import pytest

from pg_common import (
    addon_pkg,
    find_in_pkg,
    require_preload,
    run,
    sleeping_backend,
    wait_wait_event,
)

ADDON_PKG = addon_pkg("pg_wait_sampling")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "pg_wait_sampling")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_wait_sampling")
    yield conn


def profile_total(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(sum(count), 0) FROM pg_wait_sampling_profile")
        return cur.fetchone()[0]


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pg_wait_sampling.control")
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
        with sleeping_backend(3) as pid:
            found = wait_wait_event(extension, pid)
        print(f"\n  pid={pid} event={found}")
        assert found is not None, f"wait event для pid={pid} не зафиксирован"

    def test_reset_profile(self, extension):
        # профиль копится постоянно (collector семплирует даже idle-процессы),
        # поэтому после reset проверяем не ноль, а что счётчик реально упал
        with sleeping_backend(2):
            time.sleep(1.5)
        before = profile_total(extension)

        with extension.cursor() as cur:
            cur.execute("SELECT pg_wait_sampling_reset_profile()")
        after = profile_total(extension)

        print(f"\n  сумма count в profile до reset: {before}, после: {after}")
        assert before > 0, "профиль пуст - collector не собирает данные"
        assert after < before, "pg_wait_sampling_reset_profile() не очистил профиль"
