# orc-bench

Spark / Java 8 приложение для бенчмарка формата **ORC** на кластерном **Spark 3.2**.

Проект рассчитан на кластер **без изменений**: используется штатный SDP Spark 3.2 (`spark-submit` на YARN). CarbonData и BYOS Spark 3.1 в проект не входят.

## Требования

- Java 8 (совместимо с JVM кластера)
- Hadoop / YARN / HDFS кластера
- Fat JAR `orc-bench-all.jar` — приложение без Spark/Hadoop (берутся с кластера)

## Сборка

```bash
./gradlew build
```

Windows:

```bash
gradlew.bat build
```

Артефакт: `build/libs/orc-bench-all.jar` (Spark compile `3.2.1`).

Локальные unit-тесты:

```bash
./gradlew test
```

## Запуск на кластере

```bash
./scripts/submit-spark32.sh --driver-memory 8g --num-executors 16 -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

По умолчанию скрипт уже ставит `--num-executors 16`, `--executor-memory 8g`, `--executor-cores 4`, `--driver-memory 4g`.
Переопределение: флаги до `--` или env `NUM_EXECUTORS`, `EXECUTOR_MEMORY`, `EXECUTOR_CORES`, `DRIVER_MEMORY`.

Флаги `spark-submit` — до `--`, аргументы приложения — после.

Короткий smoke:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
./scripts/run-smoke.sh
```

## Формат аргументов

Все параметры передаются как `--ключ=значение`.

- Каждый аргумент должен начинаться с `--` и содержать `=`.
- Ключ и значение не могут быть пустыми.
- Регистр значения `--mode` не важен.

## Обзор пайплайна

```text
generate → validate → benchmark → report
```

| Шаг | `--mode` | Выход |
|---|---|---|
| 1. Генерация ORC | `generate` | `<orc-path>/` |
| 2. Валидация | `validate` | `<reports-path>/raw/validation/` |
| 3. Бенчмарки ORC | `benchmark` | `<reports-path>/raw/benchmark/` |
| 4. Отчёт | `report` | `<reports-path>/summary/` |

### Конфигурируемые пути HDFS

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--base-path` | `hdfs:///user/hdfs_migration_user/orc_test` | Корневой путь эксперимента |
| `--orc-path` | `<base-path>/orc` | Путь для ORC-данных |
| `--reports-path` | `<base-path>/reports` | Путь для отчётов |

Структура каталогов:

```text
<orc-path>/          # ORC-файлы
<reports-path>/
  raw/benchmark/     # сырые метрики benchmark (duration, selectivity, bytes_read)
  raw/validation/    # валидация (не затирается benchmark overwrite)
  summary/           # агрегированные отчёты
```

### Общие параметры

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `generate`, `validate`, `benchmark`, `report` |
| `--base-path` | нет | `hdfs:///user/hdfs_migration_user/orc_test` | Корневой путь |
| `--orc-path` | нет | `<base-path>/orc` | HDFS-путь для ORC |
| `--reports-path` | нет | `<base-path>/reports` | HDFS-путь для отчётов |

---

## Шаг 1. Генерация (`--mode=generate`)

Генерирует синтетический датасет и сразу записывает его в ORC.

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--target-size-tb` | `5` | Целевой объём в ТБ (дроби допустимы, например `0.01`) |
| `--seed` | `42` | Seed генератора |
| `--avg-row-bytes` | `512` | Средний размер строки |
| `--chunk-days` | `1` | Размер временного окна чанка в днях |
| `--timestamp-start` | `2024-01-01` | Начало диапазона `timestamp` |
| `--timestamp-end` | `2025-01-01` | Конец диапазона (не включительно) |
| `--target-file-size-mb` | `384` | Целевой размер выходного файла |
| `--write-partitions` | авто | Число партиций при записи |
| `--partition-by` | `event_year,event_month,event_day,log_format` | Колонки партиционирования |
| `--orc-compression` | `snappy` | `snappy`, `zstd`, `none` |
| `--orc-stripe-size-mb` | `64` | Размер ORC stripe |
| `--orc-row-group-size-mb` | `32` | Размер row group |

### Схема данных

| Колонка | Кардинальность | Описание |
|---|---|---|
| `event_id` | высокая | Уникальный ID события |
| `user_id` | высокая | ID пользователя (~50M) |
| `session_id` | высокая | ID сессии (~100M) |
| `country_code` | низкая | Код страны (50 значений) |
| `device_type` | низкая | mobile, desktop, tablet, tv, iot |
| `status` | низкая | success, failed, pending, timeout |
| `product_id` | средняя | ID продукта (~100K, Zipf) |
| `campaign_id` | средняя | ID кампании (~50K, Zipf) |
| `region_id` | средняя | ID региона (~10K, Zipf) |
| `timestamp` | — | Временная метка |
| `amount` | — | Сумма (0–10000) |
| `payload_json` | — | JSON события |
| `log_format` | низкая | json, plain_text, key_value, apache_common |
| `log_message` | — | Строка лога |

Пример:

```bash
./scripts/submit-spark32.sh -- \
  --mode=generate \
  --base-path=hdfs:///user/hdfs_migration_user/orc_test \
  --target-size-tb=1 \
  --seed=42 \
  --orc-compression=snappy
```

---

## Шаг 2. Валидация (`--mode=validate`)

Проверяет корректность ORC-датасета. Результаты — в `<reports-path>/raw/validation/`. При ошибке любой проверки job падает.

| Проверка | Описание |
|---|---|
| `row_count` | Датасет непустой |
| `low_cardinality_bounds` | low cardinality колонки в ожидаемых пределах |
| `timestamp_range` | `timestamp` в заданном диапазоне |
| `log_format_distribution` | Все форматы логов с ожидаемыми долями |
| `log_message_structure` | `log_message` не пустой; JSON начинается с `{` |

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--validation-checks` | `all` | Список проверок или `all` |
| `--validation-sample-fraction` | `0.01` | Доля выборки |
| `--log-format-share-tolerance` | `0.15` | Допуск доли `log_format` |

```bash
./scripts/submit-spark32.sh -- \
  --mode=validate \
  --base-path=hdfs:///user/hdfs_migration_user/orc_test
```

---

## Шаг 3. Бенчмарки (`--mode=benchmark`)

Запускает тесты производительности на ORC из `<orc-path>/`. Метрики пишутся в `<reports-path>/raw/benchmark/` (`spark_runtime=spark32-orc`).

Дополнительно к wall time собираются Spark input metrics: `bytes_read`, `records_read` (прокси ORC pruning / pushdown).

### Сценарии

| Сценарий | Описание |
|---|---|
| `full_scan` | Полное сканирование |
| `projection` | Выбор подмножества колонок |
| `filter_low_cardinality` | Фильтр по `country_code`, `status` |
| `filter_medium_cardinality` | Фильтр по `product_id`, `campaign_id` |
| `filter_high_cardinality` | Point lookup по `event_id`, `user_id` |
| `filter_timestamp_range` | Range-фильтр по `timestamp` (окно внутри generate-диапазона) |
| `filter_log_format` | Фильтр по `log_format` |
| `filter_combined` | Комбинированный фильтр |
| `group_by` | Агрегация `GROUP BY` |
| `text_search` | Поиск подстроки в `log_message` |

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--benchmark-warmup-runs` | `1` | Прогревочные запуски |
| `--benchmark-repeat-runs` | `3` | Измеряемые повторы (нужно ≥3 для устойчивых p50/p95) |
| `--benchmark-scenarios` | `all` | Сценарии через запятую или `all` |
| `--benchmark-timestamp-window-days` | `30` | Длина окна для `filter_timestamp_range` / `filter_combined` внутри `--timestamp-start`…`--timestamp-end` |
| `--clear-cache-between-runs` | `true` | Очистка кэша между прогонами (без cache base DF — иначе pruning не виден) |
| `--seed` | `42` | Seed для выборки значений фильтров и размещения timestamp-окна |

```bash
./scripts/submit-spark32.sh -- \
  --mode=benchmark \
  --base-path=hdfs:///user/hdfs_migration_user/orc_test \
  --seed=42 \
  --benchmark-repeat-runs=3 \
  --benchmark-timestamp-window-days=30
```

Повтор validate+benchmark+report без generate: `./scripts/run-bench-pipeline.sh`.

---

## Шаг 4. Отчёт (`--mode=report`)

Агрегирует метрики из `<reports-path>/raw/benchmark/` и validation в `<reports-path>/summary/`.
(Старые прогоны с parquet прямо в `raw/` тоже читаются как fallback.)

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--report-formats` | `parquet,csv,json,markdown` | Форматы выходных отчётов |
| `--report-name` | `benchmark-report` | Имя Markdown-отчёта |

Выход:

| Файл | Описание |
|---|---|
| `results.parquet` / `.csv` / `.json` | Агрегированные метрики |
| `<report-name>.md` | Сводка: benchmark, validation, рекомендации |

```bash
./scripts/submit-spark32.sh -- \
  --mode=report \
  --base-path=hdfs:///user/hdfs_migration_user/orc_test
```

---

## Полный прогон

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test

./scripts/submit-spark32.sh -- --mode=generate --base-path="$BASE" \
  --target-size-tb=5 --seed=42

./scripts/submit-spark32.sh -- --mode=validate --base-path="$BASE"

./scripts/submit-spark32.sh -- --mode=benchmark --base-path="$BASE"

./scripts/submit-spark32.sh -- --mode=report --base-path="$BASE"
```

Перед ТБ-прогоном сделайте smoke: `./scripts/run-smoke.sh` (`--target-size-tb=0.01`).

Подробности — в [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md).

---

## Ошибки валидации аргументов

| Ситуация | Сообщение |
|---|---|
| Не передан `--mode` | `Missing required argument: --mode=...` |
| Неизвестный режим | `Unknown mode: <value>` |
| Неверный формат аргумента | `Invalid argument: <arg>. Use --key=value` |
| Неположительное число | `Argument --<key> must be positive: <value>` |
| `timestamp-end` <= `timestamp-start` | `--timestamp-end must be greater than --timestamp-start` |
