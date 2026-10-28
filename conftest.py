import pytest

from pg_common import INFO, new_conn, run, wait_pg


@pytest.fixture(scope="session", autouse=True)
def ensure_service():
    r = run(["systemctl", "is-active", "--quiet", INFO["service"]])
    assert r.returncode == 0, (
        f"Сервис {INFO['service']} не запущен.\n"
        f"Запустите: sudo systemctl start {INFO['service']}"
    )
    assert wait_pg(), "PostgreSQL не принимает соединения"


# module, а не session: test_restart_resilience.py перезапускает сервер,
# и общее на всю сессию соединение умерло бы для файлов, идущих после него
@pytest.fixture(scope="module")
def conn(ensure_service):
    c = new_conn()
    yield c
    c.close()