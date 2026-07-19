# Lineage Analyzer

Lineage Analyzer — дипломный проект для сбора, хранения, визуализации и анализа Data Lineage. Система принимает OpenLineage-события, хранит версионную модель метаданных, строит граф зависимостей таблиц и показывает влияние изменений.

## Возможности

- прием OpenLineage events через HTTP API;
- хранение таблиц, атрибутов, jobs, трансформаций и событий в PostgreSQL;
- версионирование модели метаданных через `valid_from`, `valid_to`, `is_actual`;
- синхронизация схем из PostgreSQL, Greenplum, ClickHouse и Hadoop/Hive через Spark;
- запуск синхронизации вручную и по расписанию через APScheduler;
- web UI с интерактивным графом Data Lineage;
- просмотр свойств таблиц, атрибутов, column-level lineage, SQL-кода jobs;
- отчет изменений и затронутых downstream-объектов;
- расчет критичности узлов графа;
- ролевая модель пользователей.

## Роли

В системе есть две роли:

- `data_engineer` — полный доступ;
- `data_analyst` — доступ к графу, объектам, версиям и отчетам, но без просмотра критических узлов и без управления синхронизацией.

Первый пользователь создается автоматически при первом запуске системы с ролью `data_engineer`.

Последующие пользователи создаются через CLI:

```bash
docker compose exec app lineage-analyzer create-user analyst1 --role data_analyst
docker compose exec app lineage-analyzer create-user engineer1 --role data_engineer
```

Пароли пользователей хранятся в БД в виде PBKDF2-SHA256 hash с солью.

## Быстрый Запуск В Docker

Скопируйте пример переменных окружения:

```bash
cp .env.example .env
```

Минимальные переменные для первого запуска:

```env
POSTGRES_DB=metadata_db
POSTGRES_USER=metadata_user
POSTGRES_PASSWORD=change_me
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

LINEAGE_APP_PORT=8080
LINEAGE_ADMIN_USERNAME=admin
LINEAGE_ADMIN_PASSWORD=admin12345
LINEAGE_WAREHOUSE_ENV_FILE=/app/secrets/warehouse.env
```

Запустите приложение и PostgreSQL-хранилище метаданных:

```bash
docker compose up -d --build
```

UI будет доступен:

```text
http://127.0.0.1:8080/
```

Если `LINEAGE_ADMIN_PASSWORD` пустой, пароль первого пользователя генерируется автоматически и выводится в консоль контейнера:

```bash
docker compose logs app
```

## Docker Compose

В `docker-compose.yml` поднимаются два сервиса:

- `postgres` — PostgreSQL-хранилище метаданных;
- `app` — backend, API, scheduler и web UI.

PostgreSQL получает параметры из `.env`:

```yaml
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_PORT
```

Посмотреть итоговую конфигурацию:

```bash
docker compose config
```

Полностью удалить контейнеры без удаления данных:

```bash
docker compose down
```

Удалить контейнеры вместе с volume PostgreSQL и сохраненными secret-файлами:

```bash
docker compose down -v
```

## Подключение К БД Из Контейнера

Если внешняя БД запущена на хост-машине, из контейнера нельзя использовать `localhost`, потому что `localhost` указывает на сам контейнер приложения. Используйте:

```text
postgresql://user:password@host.docker.internal:5433/
```

Если внешняя БД запущена в другом Docker-контейнере, приложение и этот контейнер должны быть в одной Docker-сети. В DSN используйте имя сервиса или контейнера и внутренний порт:

```text
postgresql://user:password@ol_test_postgres:5432/
```

Для подключения приложения к внешней Docker-сети используется переменная:

```env
WAREHOUSE_DOCKER_NETWORK=diploma_test_db_default
```

## Секреты DSN Для Синхронизации

Пароли к внешним хранилищам, введенные в UI на вкладке синхронизации, не сохраняются в БД в открытом виде.

Приложение:

1. извлекает пароль из DSN;
2. создает переменную вида `LINEAGE_WAREHOUSE_PASSWORD_...`;
3. сохраняет пароль в env-файл `LINEAGE_WAREHOUSE_ENV_FILE`;
4. в БД записывает DSN со ссылкой на переменную окружения.

В Docker Compose этот env-файл лежит в volume `app_secrets`, поэтому пароль сохраняется после перезапуска контейнера.

## OpenLineage API

OpenLineage events нужно отправлять на endpoint:

```text
POST /openlineage/events
```

Пример:

```bash
curl -X POST http://127.0.0.1:8080/openlineage/events \
  -H "Content-Type: application/json" \
  --data @event.json
```

Этот endpoint не требует UI-cookie, чтобы внешние инструменты вроде dbt/OpenLineage могли отправлять события без браузерной авторизации.

## Основные API Endpoints

- `GET /` — web UI;
- `POST /auth/login` — вход пользователя;
- `POST /auth/logout` — выход пользователя;
- `GET /auth/me` — текущий пользователь;
- `POST /openlineage/events` — прием OpenLineage event;
- `GET /graph` — граф Data Lineage;
- `GET /tables` — актуальные таблицы;
- `GET /graph/downstream?table=<name>` — downstream-зависимости;
- `GET /analysis/critical` — критичность узлов, только `data_engineer`;
- `GET /history/table?name=<name>` — версии таблицы;
- `GET /history/job?name=<name>` — версии job;
- `GET /analysis/impact/table?...` — отчет изменений таблицы;
- `GET /analysis/impact/job?...` — отчет изменений job;
- `GET /sync-schedules` — расписания синхронизации, только `data_engineer`;
- `POST /sync-schedules` — создать расписание, только `data_engineer`;
- `POST /sync-schedules/<id>/run` — запустить синхронизацию, только `data_engineer`.

## Синхронизация Метаданных

Синхронизацию можно настроить в UI во вкладке `Синхронизация` или запускать через CLI.

Поддерживаемые источники:

- `postgresql`;
- `greenplum`;
- `clickhouse`;
- `hadoop_spark`.

### PostgreSQL

```bash
lineage-analyzer sync-postgres \
  "postgresql://user:password@postgres-host:5432/warehouse" \
  --schema public
```

В UI:

- `Тип источника`: PostgreSQL;
- `DSN подключения`: `postgresql://user:password@postgres-host:5432/`;
- `База данных / namespace`: имя БД;
- `Схема`: schema.

### Greenplum

Greenplum читается через PostgreSQL-compatible `information_schema.columns`.

```bash
lineage-analyzer sync-greenplum \
  "postgresql://user:password@gp-host:5432/warehouse" \
  --schema public
```

### ClickHouse

ClickHouse читается через HTTP API и `system.columns`.

```bash
lineage-analyzer sync-clickhouse \
  "http://user:password@clickhouse:8123/" \
  --schema analytics
```

В UI:

- `DSN подключения`: `http://user:password@clickhouse:8123/`;
- `База данных / namespace`: ClickHouse database;
- `Схема`: можно указать то же значение, для совместимости формы.

### Hadoop / Spark

Hadoop/Hive-таблицы читаются через Spark:

- создается `SparkSession`;
- включается Hive catalog через `enableHiveSupport()`;
- выполняется `SHOW TABLES IN <schema>`;
- для каждой таблицы выполняется `DESCRIBE TABLE <schema>.<table>`.

## UI

### 📊 Граф таблиц и задач

<p align="center">
  <img src="https://github.com/user-attachments/assets/ba5c7b5e-06d2-4c97-abe4-18a290e55e69" alt="Граф таблиц и задач" width="100%">
</p>

---

### 🎯 Выбор узла или ребра графа

<p align="center">
  <img src="https://github.com/user-attachments/assets/a49ab95a-021c-4683-9a56-42d82c4d85e9" alt="Выбор узла графа" width="48%">
  <img src="https://github.com/user-attachments/assets/8ae544b3-ed56-4034-98d5-1367aa7c70b2" alt="Выбор ребра графа" width="43%">
</p>

---

### 📝 Отчет об изменениях

<p align="center">
  <img src="https://github.com/user-attachments/assets/0704c65f-cb6e-413d-8183-02c9fbb1f939" alt="Отчет об изменениях" width="90%">
</p>

---

### 🚨 Подсветка критических узлов

<p align="center">
  <img src="https://github.com/user-attachments/assets/86982932-afea-4c0d-8e27-e728572ce21a" alt="Подсветка критических узлов" width="90%">
</p>

## Логи

Файловое логирование отключено. Приложение пишет в консоль только минимально необходимую информацию:

- адрес UI при старте;
- пароль первого пользователя, если он был сгенерирован автоматически;
- ошибки обработки OpenLineage events;
- ошибки синхронизации;
- сообщение о завершении sync schedule.

В Docker консольный вывод можно посмотреть так:

```bash
docker compose logs app
```

## Структура Проекта

- `lineage_analyzer/api.py` — HTTP API и отдача UI;
- `lineage_analyzer/cli.py` — CLI;
- `lineage_analyzer/repository.py` — работа с PostgreSQL-хранилищем метаданных;
- `lineage_analyzer/openlineage.py` — парсинг OpenLineage events;
- `lineage_analyzer/db_introspect.py` — introspection внешних хранилищ;
- `lineage_analyzer/services.py` — бизнес-процессы ingest, sync, impact report;
- `lineage_analyzer/graph.py` — граф зависимостей и критичность узлов;
- `lineage_analyzer/scheduler.py` — APScheduler для плановой синхронизации;
- `lineage_analyzer/ui/` — HTML, CSS, JS интерфейса.
