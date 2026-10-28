"""
запуск: pytest test_restart_resilience.py -v -s
Нужны права на systemctl restart (запуск через sudo ./version.sh).
Фикстуру conn здесь не используем: рестарт рвёт все соединения,
поэтому каждая проверка открывает новое через new_conn().
"""

import contextlib
import os
import time

import pytest

from pg_common import (
    INFO,
    audit_log_dir,
    find_marker_in_dir,
    new_conn,
    preload_libs,
    run,
    sleeping_backend,
    wait_pg,
    wait_wait_event,
)


def restart_service():
    """Обычный systemctl restart - не переустановка пакета, конфиг не трогаем."""
    r = run(["systemctl", "restart", INFO["service"]])
    assert r.returncode == 0, f"systemctl restart {INFO['service']} упал: {r.stderr}"
    assert wait_pg(), "PostgreSQL не поднялся после restart"


@contextlib.contextmanager
def connection():
    c = new_conn()
    try:
        yield c
    finally:
        c.close()


def require_preload(*libs):
    with connection() as c:
        loaded = preload_libs(c)
    missing = [lib for lib in libs if lib not in loaded]
    if missing:
        pytest.skip(f"{missing} не загружены в этом прогоне")


def audit_select(marker):
    with connection() as c, c.cursor() as cur:
        cur.execute("SET pgaudit.log = 'all'")
        cur.execute(f"SELECT 1 /* {marker} */")


class TestCronSurvivesRestart:
    """cron.job хранится в обычной таблице - определение задачи должно
    пережить рестарт, а launcher должен возобновить её выполнение."""

    def test_job_definition_and_execution_survive_restart(self):
        require_preload("pg_cron")

        with connection() as c, c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron")
            cur.execute(
                "SELECT cron.schedule('sql_auto_restart_test', '5 seconds', $$SELECT 1$$)"
            )
            jobid = cur.fetchone()[0]

        try:
            restart_service()

            with connection() as c, c.cursor() as cur:
                cur.execute("SELECT jobid FROM cron.job WHERE jobid = %s", (jobid,))
                still_defined = cur.fetchone() is not None
            print(f"\n  job {jobid} определён после рестарта: {still_defined}")
            assert still_defined, "определение задачи не пережило рестарт"

            resumed = False
            for _ in range(20):
                with connection() as c, c.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM cron.job_run_details "
                        "WHERE jobid = %s AND start_time > now() - interval '1 minute' "
                        "ORDER BY start_time DESC LIMIT 1",
                        (jobid,),
                    )
                    row = cur.fetchone()
                if row and row[0] == "succeeded":
                    resumed = True
                    break
                time.sleep(1)

            print(f"  launcher возобновил выполнение: {resumed}")
            assert resumed, "launcher не возобновил выполнение задачи после рестарта"
        finally:
            with connection() as c, c.cursor() as cur:
                cur.execute("SELECT cron.unschedule(%s)", (jobid,))
                cur.execute(
                    "DELETE FROM cron.job_run_details WHERE jobid = %s", (jobid,)
                )


class TestWaitSamplingRecoversAfterRestart:
    """История/профиль - in-memory, сброс после рестарта ОЖИДАЕМ.
    Проверяем не сохранность данных, а что расширение снова работает."""

    def test_extension_captures_new_events_after_restart(self):
        require_preload("pg_wait_sampling")

        with connection() as c, c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_wait_sampling")

        restart_service()

        with connection() as c, sleeping_backend(3) as pid:
            found = wait_wait_event(c, pid)

        print(f"\n  pid={pid} event после рестарта={found}")
        assert found is not None, "pg_wait_sampling не заработал заново после рестарта"


class TestKcacheWorksAfterRestart:
    """pg_stat_statements по умолчанию сохраняет статистику на диск при
    остановке и подгружает обратно при старте (pg_stat_statements.save=on).
    Проверяем мягко: само по себе расширение не падает и продолжает
    работать после рестарта - точную гарантию персистентности не
    хардкодим, так как поведение зависит от настройки save."""

    def test_view_still_works_after_restart(self):
        require_preload("pg_stat_statements", "pg_stat_kcache")

        with connection() as c, c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_kcache")
            cur.execute("SELECT count(*) FROM pg_class")
            cur.fetchall()
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            before = cur.fetchone()[0]

        restart_service()

        with connection() as c, c.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_class")
            cur.fetchall()
            cur.execute("SELECT count(*) FROM pg_stat_kcache()")
            after = cur.fetchone()[0]

        print(f"\n  строк до рестарта: {before}, после: {after}")
        assert after > 0, "pg_stat_kcache перестал возвращать данные после рестарта"


class TestPgAuditLogToFileSurvivesRestart:
    """Старый файл лога не должен пропасть или испортиться после рестарта,
    новые записи после рестарта должны продолжать нормально записываться."""

    def test_old_file_kept_new_writes_continue(self):
        require_preload("pgaudit", "pgauditlogtofile")

        with connection() as c:
            with c.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pgaudit")
                cur.execute("CREATE EXTENSION IF NOT EXISTS pgauditlogtofile")
            log_dir = audit_log_dir(c)
        assert log_dir is not None, "не найден GUC директории аудит-лога"

        marker_before = f"sql_auto_restart_before_{os.getpid()}_{int(time.time())}"
        audit_select(marker_before)
        assert find_marker_in_dir(
            log_dir, marker_before
        ), "не удалось зафиксировать marker_before до рестарта - тест некорректен"

        restart_service()

        old_file = find_marker_in_dir(log_dir, marker_before, timeout=0)
        print(f"\n  файл с marker_before после рестарта: {old_file}")
        assert old_file, "файл с записью ДО рестарта пропал или испортился"

        marker_after = f"sql_auto_restart_after_{os.getpid()}_{int(time.time())}"
        audit_select(marker_after)
        new_file = find_marker_in_dir(log_dir, marker_after)
        print(f"  новая запись после рестарта в: {new_file}")
        assert new_file, "новые записи не пишутся в лог после рестарта"


class TestProfileWorksAfterRestart:
    """Extension и функции pg_profile должны продолжать работать после
    рестарта без повторной установки пакета."""

    def test_take_sample_works_after_restart(self):
        with connection() as c, c.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_profile'"
            )
            available = cur.fetchone() is not None
        if not available:
            pytest.skip("pg_profile не установлен в этом прогоне")

        with connection() as c, c.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS dblink")
            cur.execute("CREATE SCHEMA IF NOT EXISTS profile_restart_test")
            cur.execute("DROP EXTENSION IF EXISTS pg_profile CASCADE")
            cur.execute("CREATE EXTENSION pg_profile SCHEMA profile_restart_test")

        try:
            restart_service()

            with connection() as c, c.cursor() as cur:
                cur.execute("SELECT result FROM profile_restart_test.take_sample()")
                rows = cur.fetchall()
            print(f"\n  take_sample после рестарта: {rows}")
            assert rows and all(
                r[0] == "OK" for r in rows
            ), "take_sample не сработал после рестарта"
        finally:
            with connection() as c, c.cursor() as cur:
                cur.execute("DROP SCHEMA IF EXISTS profile_restart_test CASCADE")
