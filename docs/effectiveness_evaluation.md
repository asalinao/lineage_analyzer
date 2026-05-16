# Оценка эффективности предложенного подхода

## Цель оценки

Целью оценки является проверка того, насколько разработанный подход повышает эффективность анализа жизненного цикла данных в корпоративном хранилище за счет автоматизированного сбора метаданных, построения графа зависимостей, анализа влияния изменений и выявления критических узлов.

Оценка выполняется по трем группам показателей:

1. Полнота функционального покрытия задач анализа жизненного цикла данных.
2. Качество построения lineage-модели на уровне таблиц и атрибутов.
3. Производительность алгоритмов анализа влияния и расчета критичности узлов.

## Функциональное покрытие

Функциональное покрытие оценивается как доля реализованных функций от набора функций, сформулированных в дипломной работе.

| Требуемая функция | Реализация в проекте | Статус |
| --- | --- | --- |
| Получение событий OpenLineage | `POST /openlineage/events`, модуль `openlineage.py`, `services.py` | Реализовано |
| Получение структуры таблиц из БД | `db_introspect.py`, поддержка PostgreSQL, Greenplum, ClickHouse, Hadoop/Hive через Spark | Реализовано |
| Хранение таблиц, атрибутов, задач и трансформаций | `repository.py`, таблицы `lineage_tables`, `lineage_attributes`, `lineage_jobs`, `lineage_transformations` | Реализовано |
| Версионирование модели метаданных | поля `version`, `valid_from`, `valid_to` | Реализовано |
| Построение графа зависимостей | `LineageGraph.dependency_graph()` | Реализовано |
| Анализ downstream-зависимостей | `LineageGraph.downstream_tables()` | Реализовано |
| Анализ влияния изменений таблиц | `LineageService.compare_table_states()` | Реализовано |
| Анализ влияния изменений задач | `LineageService.compare_job_states()` | Реализовано |
| Выявление критических узлов | `LineageGraph.critical_nodes()` | Реализовано |
| Визуализация графа и отчетов | `lineage_analyzer/ui` | Реализовано |
| Ролевая модель доступа | роли `data_engineer`, `data_analyst` | Реализовано |

Функциональное покрытие:

```text
Coverage = 11 / 11 = 1.0 = 100%
```

Следовательно, фактический функционал репозитория покрывает все ключевые функции, необходимые для анализа жизненного цикла данных в рамках поставленной задачи.

## Качество lineage-модели

Для проверки качества построения зависимостей используется расширенный эталонный набор SQL-запросов. Он включает не только простые успешные сценарии, но и случаи, в которых текущая реализация может испытывать трудности при извлечении трансформаций.

Набор составлен с учетом фактического кода:

- SQL-зависимости извлекаются модулем `SqlParser` на базе `sqlglot`;
- явно поддерживаются диалекты `postgres`, `postgresql`, `mysql`, `sqlite`, `bigquery`, `snowflake`;
- сервис не выполняет SQL-parsing для неподдерживаемых диалектов;
- `SELECT *` не раскрывается в список атрибутов;
- при неоднозначных неуточненных колонках в join-запросах связь с исходной таблицей может быть потеряна;
- CTE и промежуточные подзапросы могут быть определены как самостоятельные источники, а не как проекция исходной физической таблицы.

Для каждого запроса вручную задается ожидаемое множество связей вида:

```text
(входная таблица, входной атрибут, выходная таблица, выходной атрибут)
```

Далее результат работы `SqlParser.extract_transformations()` сравнивается с эталоном по метрикам precision, recall и F1.

Формулы:

```text
Precision = TP / (TP + FP)
Recall    = TP / (TP + FN)
F1        = 2 * Precision * Recall / (Precision + Recall)
```

где `TP` - корректно найденные зависимости, `FP` - лишние найденные зависимости, `FN` - пропущенные зависимости.

Результаты запуска по отдельным сценариям:

| Сценарий | Система | Сложность | Диалект | Ожидалось | Найдено | TP | FP | FN | Precision | Recall |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Прямая проекция атрибутов | PostgreSQL | базовый | `postgres` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| Прямая проекция атрибутов | ClickHouse | базовый | `clickhouse` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| Прямая проекция атрибутов | Hadoop/Hive | базовый | `hive` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| Join и агрегация | PostgreSQL | базовый | `postgres` | 4 | 4 | 4 | 0 | 0 | 1.00 | 1.00 |
| Join и агрегация | ClickHouse | базовый | `clickhouse` | 4 | 4 | 4 | 0 | 0 | 1.00 | 1.00 |
| Join и агрегация | Hadoop/Hive | базовый | `hive` | 4 | 4 | 4 | 0 | 0 | 1.00 | 1.00 |
| Вычисляемое выражение | PostgreSQL | базовый | `postgres` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| Вычисляемое выражение | ClickHouse | базовый | `clickhouse` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| Вычисляемое выражение | Hadoop/Hive | базовый | `hive` | 2 | 2 | 2 | 0 | 0 | 1.00 | 1.00 |
| `SELECT *` | PostgreSQL | сложная конструкция | `postgres` | 2 | 0 | 0 | 0 | 2 | 0.00 | 0.00 |
| `SELECT *` | ClickHouse | сложная конструкция | `clickhouse` | 2 | 0 | 0 | 0 | 2 | 0.00 | 0.00 |
| `SELECT *` | Hadoop/Hive | сложная конструкция | `hive` | 2 | 0 | 0 | 0 | 2 | 0.00 | 0.00 |
| Неуточненная колонка в join | PostgreSQL | сложная конструкция | `postgres` | 3 | 2 | 2 | 0 | 1 | 1.00 | 0.67 |
| Неуточненная колонка в join | ClickHouse | сложная конструкция | `clickhouse` | 3 | 2 | 2 | 0 | 1 | 1.00 | 0.67 |
| Неуточненная колонка в join | Hadoop/Hive | сложная конструкция | `hive` | 3 | 2 | 2 | 0 | 1 | 1.00 | 0.67 |
| CTE | PostgreSQL | сложная конструкция | `postgres` | 2 | 2 | 0 | 2 | 2 | 0.00 | 0.00 |
| CTE | ClickHouse | сложная конструкция | `clickhouse` | 2 | 2 | 0 | 2 | 2 | 0.00 | 0.00 |
| CTE | Hadoop/Hive | сложная конструкция | `hive` | 2 | 2 | 0 | 2 | 2 | 0.00 | 0.00 |

Агрегация по системам:

| Система | Ожидалось | Найдено | TP | FP | FN | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ClickHouse | 15 | 12 | 10 | 2 | 5 | 0.83 | 0.67 | 0.74 |
| Hadoop/Hive | 15 | 12 | 10 | 2 | 5 | 0.83 | 0.67 | 0.74 |
| PostgreSQL | 15 | 12 | 10 | 2 | 5 | 0.83 | 0.67 | 0.74 |

Агрегация по сложности:

| Группа сценариев | Ожидалось | Найдено | TP | FP | FN | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Базовые поддерживаемые сценарии | 24 | 24 | 24 | 0 | 0 | 1.00 | 1.00 | 1.00 |
| Сложные SQL-конструкции | 21 | 12 | 6 | 6 | 15 | 0.50 | 0.29 | 0.36 |

Итоговые значения:

```text
Precision = 0.8333
Recall    = 0.6667
F1        = 0.7407
```

Полученный результат показывает, что качество lineage-модели неоднородно. На простых поддерживаемых сценариях с явными колонками и алиасами модуль извлекает зависимости корректно. При этом расширенный набор выявляет ограничения текущей реализации:

- для PostgreSQL базовые сценарии извлекаются корректно, но сложные конструкции `SELECT *` и `CTE` по-прежнему обрабатываются неверно;
- для Hive и ClickHouse базовые сценарии извлекаются корректно, но сложные конструкции `SELECT *`, ambiguous join и `CTE` остаются проблемными;
- для ClickHouse и Hive итоговый профиль ошибок совпадает с PostgreSQL на сложных сценариях;
- `SELECT *` не раскрывается до набора физических атрибутов, поэтому column-level lineage не строится;
- при неуточненных колонках в запросах с несколькими входными таблицами теряется часть прямых зависимостей;
- при CTE источник может определяться как временное имя CTE, из-за чего связь с исходной таблицей не совпадает с эталоном.

Следовательно, подход эффективен при наличии OpenLineage columnLineage facets либо при SQL-запросах с поддерживаемым диалектом, явными проекциями и квалифицированными колонками. Для повышения качества модели следует добавить раскрытие `SELECT *` по известной схеме таблиц и обработку CTE/подзапросов.

Примеры некорректной работы:

- PostgreSQL, `select_star_projection`
  SQL: `select * from public.customers`
  Ожидалось:
  `warehouse.public.customers.customer_id -> warehouse.mart.customers_copy.customer_id`
  `warehouse.public.customers.email -> warehouse.mart.customers_copy.email`
  Фактически найдено: ничего. Причина: текущий парсер не раскрывает `*` по схеме входной таблицы.

- ClickHouse, `clickhouse_select_star_projection`
  SQL: `select * from analytics.customers`
  Ожидалось:
  `clickhouse.analytics.customers.customer_id -> clickhouse.mart.customers_copy.customer_id`
  `clickhouse.analytics.customers.email -> clickhouse.mart.customers_copy.email`
  Фактически найдено: ничего. Причина: текущий парсер не раскрывает `*` по схеме входной таблицы.

- Hadoop/Hive, `hadoop_spark_select_star_projection`
  SQL: `select * from dwh.customers`
  Ожидалось:
  `hive.dwh.customers.customer_id -> hive.mart.customers_copy.customer_id`
  `hive.dwh.customers.email -> hive.mart.customers_copy.email`
  Фактически найдено: ничего. Причина: текущий парсер не раскрывает `*` по схеме входной таблицы.

- PostgreSQL, `ambiguous_unqualified_join_column`
  SQL: `select id from public.customers c join public.orders o on c.id = o.id`
  Ожидалось 3 зависимости, включая dataset-level lineage для обеих таблиц.
  Фактически найдены только:
  `warehouse.public.customers.id -> warehouse.mart.customer_orders.__dataset__`
  `warehouse.public.orders.id -> warehouse.mart.customer_orders.__dataset__`
  Не найдена колонка `warehouse.mart.customer_orders.id`, потому что `id` в `select` не квалифицирована и парсер не может однозначно привязать ее к источнику.

- ClickHouse, `clickhouse_ambiguous_unqualified_join_column`
  SQL: `select id from analytics.customers c join analytics.orders o on c.id = o.id`
  Ожидалось 3 зависимости, включая dataset-level lineage для обеих таблиц.
  Фактически найдены только:
  `clickhouse.analytics.customers.id -> clickhouse.mart.customer_orders.__dataset__`
  `clickhouse.analytics.orders.id -> clickhouse.mart.customer_orders.__dataset__`
  Не найдена колонка `clickhouse.mart.customer_orders.id`, потому что `id` в `select` не квалифицирована и парсер не может однозначно привязать ее к источнику.

- Hadoop/Hive, `hadoop_spark_ambiguous_unqualified_join_column`
  SQL: `select id from dwh.customers c join dwh.orders o on c.id = o.id`
  Ожидалось 3 зависимости, включая dataset-level lineage для обеих таблиц.
  Фактически найдены только:
  `hive.dwh.customers.id -> hive.mart.customer_orders.__dataset__`
  `hive.dwh.orders.id -> hive.mart.customer_orders.__dataset__`
  Не найдена колонка `hive.mart.customer_orders.id`, потому что `id` в `select` не квалифицирована и парсер не может однозначно привязать ее к источнику.

- PostgreSQL, `cte_source_resolution`
  SQL:
  `with recent_orders as (select customer_id, amount from public.orders where amount > 0) select r.customer_id, r.amount from recent_orders r`
  Ожидалось:
  `warehouse.public.orders.customer_id -> warehouse.mart.recent_orders.customer_id`
  `warehouse.public.orders.amount -> warehouse.mart.recent_orders.amount`
  Фактически строятся ложные связи через CTE-имя:
  `recent_orders.customer_id -> warehouse.mart.recent_orders.customer_id`
  `recent_orders.amount -> warehouse.mart.recent_orders.amount`
  Причина: CTE воспринимается как самостоятельный источник, а не разворачивается обратно в `public.orders`.

- ClickHouse, `clickhouse_cte_source_resolution`
  SQL:
  `with recent_orders as (select customer_id, amount from analytics.orders where amount > 0) select r.customer_id, r.amount from recent_orders r`
  Ожидалось:
  `clickhouse.analytics.orders.customer_id -> clickhouse.mart.recent_orders.customer_id`
  `clickhouse.analytics.orders.amount -> clickhouse.mart.recent_orders.amount`
  Фактически строятся ложные связи через CTE-имя:
  `recent_orders.customer_id -> clickhouse.mart.recent_orders.customer_id`
  `recent_orders.amount -> clickhouse.mart.recent_orders.amount`
  Причина: CTE воспринимается как самостоятельный источник, а не разворачивается обратно в `analytics.orders`.

- Hadoop/Hive, `hadoop_spark_cte_source_resolution`
  SQL:
  `with recent_orders as (select customer_id, amount from dwh.orders where amount > 0) select r.customer_id, r.amount from recent_orders r`
  Ожидалось:
  `hive.dwh.orders.customer_id -> hive.mart.recent_orders.customer_id`
  `hive.dwh.orders.amount -> hive.mart.recent_orders.amount`
  Фактически строятся ложные связи через CTE-имя:
  `recent_orders.customer_id -> hive.mart.recent_orders.customer_id`
  `recent_orders.amount -> hive.mart.recent_orders.amount`
  Причина: CTE воспринимается как самостоятельный источник, а не разворачивается обратно в `dwh.orders`.

## Производительность графового анализа

Производительность оценивалась на синтетических графах жизненного цикла данных. В каждом графе:

- каждая таблица содержит 8 атрибутов;
- каждая таблица зависит от двух последующих таблиц, если они существуют;
- для каждой связи создаются column-level transformations;
- измеряется среднее время за 5 запусков.

Измерялись два основных алгоритма:

- анализ влияния изменения одного атрибута;
- расчет рейтинга критичности всех таблиц.

Результаты:

| Таблиц | Атрибутов на таблицу | Трансформаций | Impact analysis, мс | Critical nodes, мс |
| ---: | ---: | ---: | ---: | ---: |
| 25 | 8 | 376 | 0.201 | 2.996 |
| 50 | 8 | 776 | 0.374 | 21.052 |
| 100 | 8 | 1576 | 0.775 | 164.760 |

Интерпретация:

- анализ влияния изменений выполняется менее чем за 1 мс на графе из 100 таблиц и 1576 трансформаций;
- расчет критичности является более тяжелой операцией, так как для каждого узла оценивается распространение влияния, потеря связности и частота использования;
- даже для 100 таблиц расчет критичности занимает около 163 мс, что приемлемо для интерактивного анализа в веб-интерфейсе;
- рост времени расчета критичности заметно нелинейный, поэтому для крупных промышленных графов целесообразно добавить кеширование результатов или пересчет по расписанию.

## Показатель эффективности подхода

Для интегральной оценки можно использовать взвешенный показатель:

```text
E = 0.35 * Cfunc + 0.35 * F1 + 0.20 * Pimpact + 0.10 * Pcritical
```

где:

- `Cfunc` - функциональное покрытие;
- `F1` - качество восстановления зависимостей;
- `Pimpact` - нормированная производительность анализа влияния;
- `Pcritical` - нормированная производительность расчета критичности.

Для нормирования производительности принимаются целевые пороги интерактивной работы:

```text
Pimpact  = min(1, 100 мс / Timpact)
Pcritical = min(1, 1000 мс / Tcritical)
```

Для графа из 100 таблиц:

```text
Cfunc     = 1.00
F1        = 0.7407
Timpact   = 0.775 мс
Tcritical = 164.760 мс
Pimpact   = 1.00
Pcritical = 1.00

E = 0.35 * 1.00 + 0.35 * 0.7407 + 0.20 * 1.00 + 0.10 * 1.00 = 0.9092
```

Итоговый показатель эффективности на выбранном наборе сценариев составляет:

```text
E = 0.9092 = 90.92%
```

## Вывод

Проведенная оценка показывает, что предложенный подход является эффективным для анализа жизненного цикла данных в рамках реализованного проекта, но качество автоматического извлечения column-level lineage зависит от источника метаданных и сложности SQL. Система покрывает полный набор требуемых функций и обеспечивает интерактивное время выполнения ключевых аналитических операций, однако SQL-parsing требует доработок для сложных и диалектно-специфичных сценариев.

Наиболее показательным результатом является то, что анализ влияния изменений выполняется за доли миллисекунды даже при 1576 трансформациях, что позволяет использовать его непосредственно в пользовательском интерфейсе при просмотре истории версий таблиц и задач. Расчет критических узлов требует больше ресурсов, однако остается применимым для интерактивной работы на средних графах и может быть оптимизирован кешированием при росте объема метаданных. Основная зона улучшения связана не с графовыми алгоритмами, а с полнотой извлечения трансформаций из SQL.

Команда для воспроизведения оценки:

```bash
.venv/bin/python scripts/evaluate_effectiveness.py
```
