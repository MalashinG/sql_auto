# sql_auto
Автотесты для пакета `postgresql-server`
Тесты подключаются к PostgreSQL от пользователя `postgres` 
без пароля через unix-сокет. По умолчанию в `pg_hba.conf` используется метод trust

## Что тестируется

- **Package** — наличие RPM-пакета и бинарников (`postgres`, `psql`)
- **Version** — совпадение версии сервера и клиента с версией пакета
- **Service** — состояние systemd-юнита, прослушивание порта 5432
- **SQL** — подключение CRUD-операции (CREATE / INSERT / SELECT / UPDATE / DELETE) путь к unix-сокету


## Клонирование

```bash
git clone https://github.com/MalashinG/sql_auto.git
cd sql_auto
```


## Установка зависимостей
```bash
sudo dnf install python3-pytest python3-psycopg2
```

Перед запуском теста сервис должен быть запущен вручную
```bash
sudo systemctl start postgresql<версия> 
```
## Запуск теста
```bash
pytest test.py -v
pytest test.py -v -s # подробный вывод
```