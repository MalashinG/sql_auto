# sql_auto

Автотесты для пакета `postgresql-server` и аддона `pg_profile` под ROSA Linux.

Тесты подключаются к PostgreSQL от пользователя `postgres` без пароля через unix-сокет. По умолчанию в `pg_hba.conf` используется метод trust.

Скрипт `version.sh` перебирает версии PostgreSQL 15, 16, 17, 18 - на каждой ставит пакеты, запускает оба набора тестов и удаляет перед переходом к следующей версии.

## Что тестируется

### test.py - базовый пакет

* **Package** - наличие RPM-пакета и бинарников (`postgres`, `psql`)
* **Version** - совпадение версии сервера/psql с версией пакета, `server_version_num`, проверка что datadir проинициализирован именно этой версией
* **Service** - состояние systemd-юнита, прослушивание порта 5432
* **SQL** - CRUD-операции (CREATE / INSERT / SELECT / UPDATE / DELETE), путь к unix-сокету
* **IndependentVerification** - данные, вставленные через psycopg2, реально видны из нового соединения и через отдельный процесс `psql` (не просто в памяти исходного курсора)
* **Extensions** - smoke-проверка что `pg_profile` грузится через `CREATE EXTENSION`
* **DumpRestore** - `pg_dump` не падает, дамп содержит тестовые данные

### test_pg_profile.py - функциональность pg_profile

* **Package** - control-файл, sql-скрипты, зависимость `dblink`, наличие `-devel` пакета, соответствие `pg_config --sharedir` реальному расположению файлов
* **Extension** - установка в отдельную схему `profile`, совпадение версии extension с версией rpm-пакета, наличие ключевых функций (`take_sample`, `get_report`, `show_samples`, `show_servers`), автосоздание сервера `local`
* **Functionality** - реальный сбор снапшотов (`take_sample`), генерация HTML-отчёта и проверка его содержимого, корректная очистка схемы через `DROP SCHEMA CASCADE`

Сгенерированные отчёты сохраняются в `./pg_profile_reports/` (путь переопределяется переменной `PG_PROFILE_REPORTS_DIR`).

## Клонирование

```bash
git clone https://github.com/MalashinG/sql_auto.git
cd sql_auto
```

## Установка зависимостей

```bash
sudo dnf install python3-pytest python3-pip
python3 -m pip install psycopg2-binary
```

На некоторых сборках ROSA пакет `python3-psycopg2` в репозиториях отсутствует или устарел - надёжнее ставить через pip.

## Запуск

```bash
sudo ./version.sh
```

Скрипт для каждой версии PostgreSQL:
1. Устанавливает `-server`
2. Устанавливает `-contrib`, `-pg_profile`, `-devel`
3. Запускает сервис, гоняет `test.py` и `test_pg_profile.py`
4. Останавливает сервис и удаляет все пакеты версии

Ручной запуск на уже поднятом сервере:

```bash
python3 -m pytest test.py -v -s
python3 -m pytest test_pg_profile.py -v -s
```

## Требования

* `sudo -u postgres` без пароля для проверки datadir через `pg_controldata` (иначе тест скипается - см. `test_datadir_initialized_by_this_version`).