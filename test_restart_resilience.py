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


def restart_service():
    """Обычный systemctl restart — не переустановка пакета, конфиг не трогаем."""
    r = run(["systemctl", "restart", INFO["service"]])
    assert r.returncode == 0, f"systemctl restart {INFO['service']} упал: {r.stderr}"
    assert wait_pg(), "PostgreSQL не поднялся после restart"


def new_conn():
    c = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME)
    c.autocommit = True
    return c


def preload_has(*libs):
    c = new_conn()
    try:
        with c.cursor() as cur:
            cur.execute("SHOW shared_preload_libraries")
            preload = cur.fetchone()[0]
    finally:
        c.close()
    return all(lib in preload for lib in libs)


@pytest.fixture(scope="session", autouse=True)
def ensure_service():
    r = run(["systemctl", "is-active", "--quiet", INFO["service"]])
    assert r.returncode == 0, f"{INFO['service']} не запущен"
    assert wait_pg(), "PostgreSQL не отвечает"


class TestCronSurvivesRestart:
    """cron.job хранится в обычной таблице — определение задачи должно
    пережить рестарт, а launcher должен возобновить её выполнение."""

    def test_job_definition_and_execution_survive_restart(self):
        if not preload_has("pg_cron"):
            pytest.skip("pg_cron не загружен в этом прогоне")

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
            cur.execute(
                "SELECT cron.schedule('sql_auto_restart_test', '5 seconds', $$SELECT 1$$)"
            )
            jobid = cur.fetchone()[0]
        c.close()

        try:
            restart_service()

            c = new_conn()
            with c.cursor() as cur:
                cur.execute("SELECT jobid FROM cron.job WHERE jobid = %s", (jobid,))
                still_defined = cur.fetchone() is not None
            c.close()
            print(f"\n  job {jobid} определён после рестарта: {still_defined}")
            assert still_defined, "определение задачи не пережило рестарт"

            resumed = False
            for _ in range(20):
                c = new_conn()
                with c.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM cron.job_run_details "
                        "WHERE jobid = %s AND start_time > now() - interval '1 minute' "
                        "ORDER BY start_time DESC LIMIT 1",
                        (jobid,),
                    )
                    row = cur.fetchone()
                c.close()
                if row and row[0] == "succeeded":
                    resumed = True
                    break
                time.sleep(1)

            print(f"  launcher возобновил выполнение: {resumed}")
            assert resumed, "launcher не возобновил выполнение задачи после рестарта"
        finally:
            c = new_conn()
            with c.cursor() as cur:
                cur.execute("SELECT cron.unschedule(%s)", (jobid,))
                cur.execute(
                    "DELETE FROM cron.job_run_details WHERE jobid = %s", (jobid,)
                )
            c.close()


class TestWaitSamplingRecoversAfterRestart:
    """История/профиль — in-memory, сброс после рестарта ОЖИДАЕМ.
    Проверяем не сохранность данных, а что расширение снова работает."""

    def test_extension_captures_new_events_after_restart(self):
        if not preload_has("pg_wait_sampling"):
            pytest.skip("pg_wait_sampling не загружен в этом прогоне")

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_wait_sampling")
        c.close()

        restart_service()

        import threading

        result = {}

        def sleeper():
            sc = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=PG_DBNAME
            )
            with sc.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                result["pid"] = cur.fetchone()[0]
                cur.execute("SELECT pg_sleep(3)")
            sc.close()

        t = threading.Thread(target=sleeper)
        t.start()
        time.sleep(0.5)
        pid = result.get("pid")

        found = None
        for _ in range(20):
            c = new_conn()
            with c.cursor() as cur:
                cur.execute(
                    "SELECT event_type, event FROM pg_wait_sampling_current WHERE pid = %s",
                    (pid,),
                )
                row = cur.fetchone()
            c.close()
            if row and row[0] == "Timeout":
                found = row
                break
            time.sleep(0.2)
        t.join()

        print(f"\n  pid={pid} event после рестарта={found}")
        assert found is not None, "pg_wait_sampling не заработал заново после рестарта"


class TestKcacheWorksAfterRestart:
    """pg_stat_statements по умолчанию сохраняет статистику на диск при
    остановке и подгружает обратно при старте (pg_stat_statements.save=on).
    Проверяем мягко: само по себе расширение не падает и продолжает
    работать после рестарта — точную гарантию персистентности не
    хардкодим, так как поведение зависит от настройки save."""

    def test_view_still_works_after_restart(self):
        if not preload_has("pg_stat_kcache"):
            pytest.skip("pg_stat_kcache не загружен в этом прогоне")

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_kcache")
            cur.execute("SELECT count(*) FROM pg_class")
            cur.fetchall()
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            before = cur.fetchone()[0]
        c.close()

        restart_service()

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_class")
            cur.fetchall()
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            after = cur.fetchone()[0]
        c.close()

        print(f"\n  строк до рестарта: {before}, после: {after}")
        assert after > 0, "pg_stat_kcache перестал возвращать данные после рестарта"


class TestPgAuditLogToFileSurvivesRestart:
    """Старый файл лога не должен пропасть или испортиться после рестарта,
    новые записи после рестарта должны продолжать нормально записываться."""

    def test_old_file_kept_new_writes_continue(self):
        if not preload_has("pgaudit", "pgauditlogtofile"):
            pytest.skip("pgaudit+pgauditlogtofile не загружены в этом прогоне")

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile")
            cur.execute("SHOW data_directory")
            pgdata = cur.fetchone()[0]
            cur.execute(
                "SELECT name, setting FROM pg_settings WHERE name ILIKE '%pgaudit%' AND name ILIKE '%director%'"
            )
            dir_row = cur.fetchone()
        c.close()
        assert dir_row is not None, "не найден GUC директории аудит-лога"
        dir_value = dir_row[1]
        log_dir = (
            dir_value if dir_value.startswith("/") else os.path.join(pgdata, dir_value)
        )

        marker_before = f"sql_auto_restart_before_{os.getpid()}_{int(time.time())}"
        c = new_conn()
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker_before} */")
        c.close()

        found_before = False
        for _ in range(10):
            import glob

            files = glob.glob(os.path.join(log_dir, "*"))
            for f in files:
                if not os.path.isfile(f):
                    continue
                try:
                    with open(f, encoding="utf-8", errors="ignore") as fh:
                        if marker_before in fh.read():
                            found_before = True
                            break
                except OSError:
                    continue
            if found_before:
                break
            time.sleep(0.5)
        assert (
            found_before
        ), "не удалось зафиксировать marker_before до рестарта — тест некорректен"

        restart_service()

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile")
        c.close()

        import glob

        old_file_intact = False
        for f in glob.glob(os.path.join(log_dir, "*")):
            if not os.path.isfile(f):
                continue
            try:
                with open(f, encoding="utf-8", errors="ignore") as fh:
                    if marker_before in fh.read():
                        old_file_intact = True
                        break
            except OSError:
                continue
        print(f"\n  старый файл с marker_before цел после рестарта: {old_file_intact}")
        assert old_file_intact, "файл с записью ДО рестарта пропал или испортился"

        marker_after = f"sql_auto_restart_after_{os.getpid()}_{int(time.time())}"
        c = new_conn()
        with c.cursor() as cur:
            cur.execute("SET pgaudit.log = 'all'")
            cur.execute(f"SELECT 1 /* {marker_after} */")
        c.close()

        found_after = False
        for _ in range(10):
            for f in glob.glob(os.path.join(log_dir, "*")):
                if not os.path.isfile(f):
                    continue
                try:
                    with open(f, encoding="utf-8", errors="ignore") as fh:
                        if marker_after in fh.read():
                            found_after = True
                            break
                except OSError:
                    continue
            if found_after:
                break
            time.sleep(0.5)
        print(f"  новая запись после рестарта появилась: {found_after}")
        assert found_after, "новые записи не пишутся в лог после рестарта"


class TestProfileWorksAfterRestart:
    """Extension и функции pg_profile должны продолжать работать после
    рестарта без повторной установки пакета."""

    def test_take_sample_works_after_restart(self):
        c = new_conn()
        with c.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_profile'"
            )
            available = cur.fetchone() is not None
        c.close()
        if not available:
            pytest.skip("pg_profile не установлен в этом прогоне")

        c = new_conn()
        with c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS dblink")
            cur.execute("CREATE SCHEMA IF NOT EXISTS profile_restart_test")
            cur.execute("DROP EXTENSION IF EXISTS pg_profile CASCADE")
            cur.execute("CREATE EXTENSION pg_profile SCHEMA profile_restart_test")
        c.close()

        restart_service()

        c = new_conn()
        try:
            with c.cursor() as cur:
                cur.execute("SELECT result FROM profile_restart_test.take_sample()")
                rows = cur.fetchall()
            print(f"\n  take_sample после рестарта: {rows}")
            assert rows and all(
                r[0] == "OK" for r in rows
            ), "take_sample не сработал после рестарта"
        finally:
            with c.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS profile_restart_test CASCADE")
            c.close()
