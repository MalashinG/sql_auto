"""
Общие хелперы для всех тестов: определение установленной версии,
параметры подключения, ожидание сервера, поиск меток в логах.
Фикстуры лежат в conftest.py, здесь - только обычные функции.
"""

import contextlib
import glob
import os
import re
import subprocess
import threading
import time

import psycopg2
import pytest

# стандартные имена переменных libpq - можно переопределить снаружи
PG_HOST = os.environ.get("PGHOST", "localhost")
PG_PORT = int(os.environ.get("PGPORT", "5432"))
PG_USER = os.environ.get("PGUSER", "postgres")
PG_DBNAME = os.environ.get("PGDATABASE", "postgres")


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
        pytest.exit(
            "rpm не найден - тесты рассчитаны на RPM-based дистрибутив (ROSA Linux)"
        )

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

    pytest.exit(
        "Активный postgresql*-server не найден.\n"
        "Установите и запустите пакет, например: dnf install postgresql(версия)-server"
    )


INFO = _detect()


def addon_pkg(addon):
    """postgresql16-server + 'pg_cron' -> postgresql16-pg_cron"""
    return INFO["pkg_name"][: -len("-server")] + "-" + addon


def pg_bin(name):
    for path in [os.path.join(INFO["bin_dir"], name), f"/usr/bin/{name}"]:
        if os.path.exists(path):
            return path
    return name


def new_conn(dbname=PG_DBNAME, **kw):
    c = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, dbname=dbname, **kw
    )
    c.autocommit = True
    return c


def wait_pg(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            new_conn(connect_timeout=2).close()
            return True
        except psycopg2.OperationalError:
            time.sleep(1)
    return False


def preload_libs(conn):
    """shared_preload_libraries как множество имён.
    Сравнивать подстрокой нельзя: 'pgaudit' in 'pgauditlogtofile' == True."""
    with conn.cursor() as cur:
        cur.execute("SHOW shared_preload_libraries")
        value = cur.fetchone()[0]
    return {lib.strip().strip('"') for lib in value.split(",") if lib.strip()}


def require_preload(conn, *libs):
    missing = [lib for lib in libs if lib not in preload_libs(conn)]
    if missing:
        pytest.skip(
            f"в shared_preload_libraries нет: {missing} - добавьте "
            f"shared_preload_libraries = '{','.join(libs)}' в postgresql.conf "
            "и перезапустите сервер"
        )


def find_in_pkg(pkg, suffix):
    r = run(["rpm", "-ql", pkg])
    return next((l for l in r.stdout.splitlines() if l.endswith(suffix)), None)


def journal_since():
    """Отметка времени для journalctl --since: берём с запасом в 2 секунды,
    чтобы не потерять строки из-за округления до секунд."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 2))


def journal_has(marker, since, *also, timeout=5):
    """Ждёт, пока в журнале сервиса (начиная с since) появится строка,
    содержащая marker и все подстроки из also."""
    deadline = time.time() + timeout
    while True:
        r = run(
            ["journalctl", "-u", INFO["service"], "--no-pager", "--since", since]
        )
        for line in r.stdout.splitlines():
            if marker in line and all(s in line for s in also):
                return True
        if time.time() >= deadline:
            return False
        time.sleep(0.5)


def find_marker_in_dir(log_dir, marker, timeout=10):
    """Ищет marker во всех файлах log_dir, ждёт до timeout секунд.
    Возвращает путь файла с меткой или None."""
    deadline = time.time() + timeout
    while True:
        for path in glob.glob(os.path.join(log_dir, "*")):
            if not os.path.isfile(path):
                continue
            try:
                with open(path, encoding="utf-8", errors="ignore") as fh:
                    if marker in fh.read():
                        return path
            except OSError:
                continue
        if time.time() >= deadline:
            return None
        time.sleep(0.5)


def audit_log_dir(conn):
    """Каталог аудит-лога pgauditlogtofile (GUC *pgaudit*director*),
    относительный путь считается от data_directory. None если GUC нет."""
    with conn.cursor() as cur:
        cur.execute("SHOW data_directory")
        pgdata = cur.fetchone()[0]
        cur.execute(
            "SELECT setting FROM pg_settings "
            "WHERE name ILIKE '%pgaudit%' AND name ILIKE '%director%'"
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row[0] if row[0].startswith("/") else os.path.join(pgdata, row[0])


@contextlib.contextmanager
def sleeping_backend(seconds=3):
    """Отдельный backend, который висит в pg_sleep. Отдаёт его pid.
    Поток всегда join'ится, ошибки из потока пробрасываются в тест."""
    ready = threading.Event()
    state = {}

    def worker():
        try:
            c = new_conn()
            try:
                with c.cursor() as cur:
                    cur.execute("SELECT pg_backend_pid()")
                    state["pid"] = cur.fetchone()[0]
                    ready.set()
                    cur.execute("SELECT pg_sleep(%s)", (seconds,))
            finally:
                c.close()
        except Exception as e:  # noqa: BLE001 - отдаём в основной поток
            state["error"] = e
            ready.set()

    t = threading.Thread(target=worker)
    t.start()
    try:
        assert ready.wait(10), "backend не стартовал за 10 секунд"
        if "error" in state:
            raise state["error"]
        yield state["pid"]
    finally:
        t.join()
    if "error" in state:
        raise state["error"]


def wait_wait_event(conn, pid, event_type="Timeout", timeout=4):
    """Ждёт, пока pg_wait_sampling_current покажет pid в нужном типе ожидания."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, event FROM pg_wait_sampling_current WHERE pid = %s",
                (pid,),
            )
            row = cur.fetchone()
        if row and row[0] == event_type:
            return row
        time.sleep(0.2)
    return None
