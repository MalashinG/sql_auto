"""
запуск: pytest test_pg_cron.py -v -s
"""

import os
import time

import pytest

from pg_common import addon_pkg, find_in_pkg, require_preload, run

ADDON_PKG = addon_pkg("pg_cron")


@pytest.fixture(scope="module")
def extension(conn):
    require_preload(conn, "pg_cron")
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
    yield conn


class TestPackage:

    def test_addon_installed(self):
        r = run(["rpm", "-q", ADDON_PKG])
        print(f"\n  {r.stdout.strip()}")
        assert r.returncode == 0

    def test_control_file(self):
        path = find_in_pkg(ADDON_PKG, "pg_cron.control")
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
        assert succeeded, "Job не выполнился за 20 секунд"

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
