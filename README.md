# Lineage Analyzer

Программная часть дипломного проекта: подсистема сбора, хранения и анализа метаданных жизненного цикла данных в корпоративном хранилище.

## Возможности

- прием событий OpenLineage в формате JSON;
- сохранение истории событий выполнения задач;
- версионное хранение таблиц, атрибутов, задач и трансформаций;
- синхронизация структуры таблиц из PostgreSQL-хранилища;
- анализ зависимых таблиц и атрибутов на основе обхода графа;
- расчет критических узлов конвейера данных по влиянию, потере связности и частоте использования;
- CLI, JSON API и web-интерфейс графа Data Lineage.

## Требования

- Python 3.11+;
- PostgreSQL 13+;
- Python-драйвер `psycopg`.

Установка зависимостей:

```bash
python3 -m pip install -e .
```

Локальная БД метаданных и web-приложение описаны в `docker-compose.yml`, а параметры подключения хранятся отдельно в локальном `.env`.

```bash
cp .env.example .env
```

После этого заполните значения в `.env` и запустите весь стек:

```bash
docker compose up -d --build
```

Приложение будет доступно по адресу:

```text
http://127.0.0.1:8080/
```

Если `LINEAGE_ADMIN_PASSWORD` в `.env` пустой, первый пользователь `data_engineer` создается автоматически, а сгенерированный пароль печатается в логи контейнера:

```bash
docker compose logs app
```

Файл `.env` добавлен в `.gitignore` и не должен попадать в репозиторий. CLI автоматически читает `.env`; также подключение можно переопределить через `--dsn` или `LINEAGE_DATABASE_URL`.

Создание последующих пользователей:

```bash
docker compose exec app lineage-analyzer create-user analyst1 --role data_analyst
docker compose exec app lineage-analyzer create-user engineer1 --role data_engineer
```

Пароли к внешним PostgreSQL-хранилищам, которые вводятся в UI на вкладке синхронизации, не сохраняются в БД в открытом виде. Приложение заменяет пароль в DSN на `${LINEAGE_WAREHOUSE_PASSWORD_...}` и сохраняет реальный пароль в env-файл `LINEAGE_WAREHOUSE_ENV_FILE`. В Docker Compose этот файл лежит в volume `app_secrets`, поэтому пароль доступен после перезапуска контейнера.

Если синхронизируемая БД запущена на хост-машине, из контейнера нельзя использовать `localhost`, потому что он указывает на сам контейнер. Используйте адрес:

```text
postgresql://user:password@host.docker.internal:5433/
```

## Быстрый запуск

```bash
python3 -m lineage_analyzer.cli ingest-openlineage examples/openlineage_event.json
python3 -m lineage_analyzer.cli downstream dwh.orders
python3 -m lineage_analyzer.cli critical
```

Web UI и JSON API:

```bash
python3 -m lineage_analyzer.cli serve --port 8080
```

UI графа зависимостей:

```text
http://127.0.0.1:8080/
```

В интерфейсе доступны темная тема, поиск таблиц, интерактивный SVG-граф, pan/zoom, ручное перетаскивание узлов и просмотр downstream-зависимостей выбранной таблицы.

Основные эндпоинты:

- `POST /openlineage/events` - прием события OpenLineage;
- `GET /graph` - полный граф зависимостей для UI;
- `GET /tables` - актуальные таблицы модели данных;
- `GET /graph/downstream?table=dwh.orders` - зависимые таблицы и атрибуты;
- `GET /analysis/critical` - рейтинг критических узлов;
- `GET /health` - проверка доступности сервиса.

Синхронизация структуры таблиц из PostgreSQL-хранилища:

```bash
python3 -m lineage_analyzer.cli \
  --dsn "$LINEAGE_DATABASE_URL" \
  sync-postgres "postgresql://user:password@localhost:5432/warehouse" \
  --schema public
```

Также доступны интроспекторы:

```bash
lineage-analyzer sync-greenplum "postgresql://user:password@gp-host:5432/warehouse" --schema public
lineage-analyzer sync-clickhouse "http://user:password@clickhouse:8123/" --schema analytics
lineage-analyzer sync-hadoop-spark "local[*]" --schema default
```

Для расписаний синхронизации в UI выберите тип источника. Для PostgreSQL/Greenplum поле `База данных / namespace` используется как имя БД подключения, а поле `Схема` как schema. Для ClickHouse `База данных / namespace` используется как ClickHouse database. Для Hadoop/Spark поле `DSN подключения` задает Spark master (`local[*]`, `spark://spark-master:7077`), а поле `Схема` задает Hive database, из которой Spark читает таблицы через Hive catalog.

## Соответствие ТЗ

В проекте реализованы модули, описанные в специальном разделе:

- `openlineage.py` - получение и первичная обработка событий OpenLineage;
- `repository.py` - PostgreSQL-хранилище метаданных и версионное обновление модели;
- `db_introspect.py` - получение метаданных из БД;
- `graph.py` - impact analysis и расчет критических узлов;
- `api.py` - HTTP-сервер на стандартной библиотеке Python, JSON API и выдача UI;
- `cli.py` - командный интерфейс для запуска сценариев вручную.

## Подробная документация

Полное описание процессов находится в [docs/processes.md](docs/processes.md).
