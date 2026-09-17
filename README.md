# orc-bench

Факторный бенчмарк **ORC + HDFS + Spark 3.2 (+ Hive/Tez/LLAP)** на штатном SDP-кластере.

Java 8 / Spark SQL приложение: `generate → validate → benchmark → report`.  
Кластер **не меняем** — `spark-submit` на YARN. CarbonData и BYOS Spark 3.1 вне scope.

| Документ | Содержание |
|---|---|
| [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md) | Пошаговый запуск на edge-ноде |
| [docs/orc-hive-bench-plan.md](docs/orc-hive-bench-plan.md) | План реализации P0–P10 |
| [docs/ORC + HDFS + Spark + Hive benchmark.md](docs/ORC%20+%20HDFS%20+%20Spark%20+%20Hive%20benchmark.md) | Методика факторного эксперимента |

## Принципы

1. **Один фактор за раз** (partition / sort / bloom / stride / stripe / compression / file size / Spark toggles).
2. **Cold и warm не усреднять** (`--cache-state=cold|warm`).
3. Layout выбирают на Dataset **S/M**; финальный SLA — на Dataset **L**.
4. Hive читает **те же** ORC-файлы, что и Spark (`layouts/best_orc/orc`).
5. Один датасет ≤ **500 GB** (`--target-size-tb=0.5`). На HDFS ~**700 GB** свободно — лишние layout-копии удалять.

## Масштабы датасетов

| Имя | `--target-size-tb` | ≈ объём | Назначение |
|---|---|---|---|
| Smoke | `0.01` | ~10 GB | проверка пайплайна |
| **S** | `0.1` | ~100 GB | layout-факторы |
| **M** | `0.25` | ~250 GB | top-N подтверждение |
| **L** | `0.5` | **~500 GB (макс.)** | BEST_ORC, Spark S0/S1, Hive, concurrency, SLA |

Скрипты `run-factor.sh` / `run-layout-sweep.sh` / `run-sla-matrix.sh` отказывают при `TARGET_SIZE_TB > 0.5`.

## Требования и сборка

- Java 8, Hadoop / YARN / HDFS кластера
- Артефакт для submit: **`build/libs/orc-bench-all.jar`** (Shadow JAR; Spark/Hadoop — с кластера)
- Thin JAR `orc-bench-0.1.0-SNAPSHOT.jar` на кластер **не** копировать

```bash
./gradlew build    # или gradlew.bat build
./gradlew test
```

## Быстрый старт

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=build/libs/orc-bench-all.jar   # на edge: ~/orc-bench/orc-bench-all.jar

# 1) Smoke
./scripts/run-smoke.sh

# 2) Baseline B0 на Dataset S
TARGET_SIZE_TB=0.1 ./scripts/run-factor.sh --layout=b0

# 3) Layout-факторы по группам (не SWEEP=all без очистки HDFS)
TARGET_SIZE_TB=0.1 SWEEP=partition ./scripts/run-layout-sweep.sh

# 4) Freeze BEST_ORC → Spark matrix → Hive → SLA на L
TARGET_SIZE_TB=0.1 ./scripts/run-factor.sh --layout=best_orc
./scripts/run-spark-exec-matrix.sh
./scripts/hive/run-hive-factor.sh h0
# очистить лишние layouts/*, затем:
TARGET_SIZE_TB=0.5 ./scripts/run-factor.sh --layout=best_orc
TARGET_SIZE_TB=0.5 ./scripts/run-sla-matrix.sh
```

Порядок R&D:

```text
smoke → B0 → layout sweep (S) → BEST_ORC → Spark S0/S1
      → Hive H0–H4 → concurrency → SLA (L ≤ 500 GB)
```

## Скрипты

| Скрипт | Назначение |
|---|---|
| [`scripts/submit-spark32.sh`](scripts/submit-spark32.sh) | один YARN job (`--mode=…`); ресурсы до `--` |
| [`scripts/run-smoke.sh`](scripts/run-smoke.sh) | e2e smoke `0.01` |
| [`scripts/run-factor.sh`](scripts/run-factor.sh) | **основной** фактор: generate→validate→benchmark→report для одного layout |
| [`scripts/run-layout-sweep.sh`](scripts/run-layout-sweep.sh) | пачка layout’ов (P3–P6) |
| [`scripts/run-spark-exec-matrix.sh`](scripts/run-spark-exec-matrix.sh) | S0/S1 + toggles pushdown/AQE/DPP/CBO на BEST_ORC |
| [`scripts/hive/run-hive-factor.sh`](scripts/hive/run-hive-factor.sh) | Hive/Tez/LLAP H0–H4 на тех же ORC |
| [`scripts/run-concurrency.sh`](scripts/run-concurrency.sh) | concurrent 1/5/10/25/50 |
| [`scripts/run-sla-matrix.sh`](scripts/run-sla-matrix.sh) | финальная SLA-матрица на L |
| [`scripts/run-bloom-ab.sh`](scripts/run-bloom-ab.sh) | legacy bloom A/B (`orc` vs `orc_bloom`) |
| [`scripts/run-bench-pipeline.sh`](scripts/run-bench-pipeline.sh) | validate→benchmark→report без generate |

`submit-spark32.sh`: флаги `spark-submit` **до** `--`, аргументы приложения **после**.  
Дефолты: `NUM_EXECUTORS=16`, `EXECUTOR_MEMORY=8g`, `EXECUTOR_CORES=4`, `DRIVER_MEMORY=4g`.  
Hive/HBase credentials на submit отключены (для Spark ORC); Hive-бенчмарк — через Beeline.

## Режимы приложения

Формат аргументов: **`--ключ=значение`**.

| `--mode` | Действие | Выход |
|---|---|---|
| `generate` | синтетика → ORC + dictionary (Q10) | `<orc-path>/`, `<dictionary-path>/` |
| `validate` | качество + bloom metadata | `…/raw/validation/` |
| `benchmark` | query suite + метрики/SLA | `…/raw/benchmark/` |
| `report` | агрегация | `…/summary/` |

### Пути HDFS

Без `--layout-id` (или `default`):

```text
$BASE/orc/
$BASE/dictionary/
$BASE/reports/raw/{benchmark,validation}/
$BASE/reports/summary/
```

С `--layout-id=<id>` (рекомендуется для факторов):

```text
$BASE/layouts/<id>/orc/
$BASE/layouts/<id>/dictionary/
$BASE/layouts/<id>/reports/raw/...
$BASE/layouts/<id>/reports/summary/
```

Переопределение: `--orc-path`, `--dictionary-path`, `--reports-path`, `--reports-benchmark-path`, `--reports-validation-path`.

### Layout’ы `run-factor.sh`

| ID | Смысл |
|---|---|
| `b0` | ORC baseline (без bloom/sort) |
| `d0` / `d1` / `d2` | partitioning none / date / date+hour |
| `e1` / `e2` / `e3` | sort high / medium / timestamp+high |
| `f0`–`f3`, `g005`/`g001`/`g0001` | bloom columns / FPP |
| `h5k`…`h50k`, `i64`…`i256` | stride / stripe |
| `jsnappy` / `jzstd` / `jzlib` | compression |
| `k32` / `k256` / `k1024` | target file size |
| `best_orc` | зафиксированный профиль победителей |
| `s0` / `s1` | Spark baseline OFF / optimized ON (читает `best_orc`) |

## Схема данных

| Колонка | Card. | Роль в Q1–Q10 |
|---|---|---|
| `event_id`, `user_id` | высокая | Q2, Q5, bloom |
| `product_id`, `campaign_id` | средняя | Q3, Q5, Q10 |
| `country_code`, `status`, `device_type`, `log_format` | низкая | Q4 |
| `timestamp` | — | Q6 |
| `log_message` / `payload_json` | — | column pruning, text_search |
| `event_year` / `month` / `day` / `hour` | — | partition prune (Q1) |

## Query suite

| Сценарий | Документ | Что меряем |
|---|---|---|
| `partition_prune` | Q1 | partition pruning |
| `filter_high_cardinality` | Q2 | equality high / bloom / min-max |
| `filter_medium_cardinality` | Q3 | medium |
| `filter_low_cardinality` | Q4 | low (bloom часто слаб) |
| `filter_in` | Q5 | `IN` |
| `filter_timestamp_range` | Q6 | range |
| `projection` / `full_scan` | Q7 | column pruning |
| `group_by` / `group_by_heavy` | Q8 / Q9 | aggregation / AQE |
| `join_dictionary` | Q10 | JOIN + DPP |
| `filter_log_format`, `filter_combined`, `text_search` | доп. | в `all`, не в `doc` |

`--benchmark-scenarios=doc` — suite Q1–Q10; `all` — полный набор.

## Метрики

Raw parquet / отчёт:

| Поле | Смысл |
|---|---|
| `duration_ms`, p50/p95/p99 | latency |
| `bytes_read`, `records_read`, `selectivity` | I/O / pruning |
| `layout_id`, `engine`, `cache_state` | фактор эксперимента |
| `scan_ratio` | `bytes_read / dataset_bytes` |
| `sla_ok` | `duration_ms ≤ --sla-threshold-ms` (дефолт 3000) |

Markdown: Benchmark Summary, **SLA ≤ 3s**, Bloom comparison, Validation, Recommendations.

## Основные CLI-параметры

### Generate / ORC write

| Параметр | Дефолт | Описание |
|---|---|---|
| `--target-size-tb` | `0.5` | объём; **макс. 0.5** на кластере |
| `--layout-id` | `default` | → `layouts/<id>/…` |
| `--partition-by` | date + `log_format` | или `none` |
| `--orc-sort-columns` | `none` | sortWithinPartitions |
| `--orc-bloom-filter-columns` | high+medium ids | или `none` |
| `--orc-bloom-filter-fpp` | `0.05` | FPP |
| `--orc-row-index-stride` | `10000` | ORC stride |
| `--orc-stripe-size-mb` | `64` | stripe |
| `--orc-compression` | `snappy` | `snappy` / `zstd` / `zlib` / `none` |
| `--target-file-size-mb` | `384` | целевой размер файла |
| `--seed` | `42` | воспроизводимость |

У **generate** и **validate** одинаковый `--orc-bloom-filter-columns`.

### Benchmark / Spark exec

| Параметр | Дефолт | Описание |
|---|---|---|
| `--benchmark-scenarios` | `all` | CSV / `all` / `doc` |
| `--benchmark-warmup-runs` | `1` | прогрев |
| `--benchmark-repeat-runs` | `3` | измерения (≥3 для p50/p95) |
| `--clear-cache-between-runs` | `true` | cold protocol |
| `--cache-state` | `cold` | метка `cold` / `warm` |
| `--engine` | `spark` | `spark` / `hive_tez` / `hive_llap` |
| `--sla-threshold-ms` | `3000` | порог SLA |
| `--spark-orc-filter-pushdown` | `true` | |
| `--spark-orc-vectorized` | `true` | |
| `--spark-aqe` | `true` | |
| `--spark-dpp` | `true` | |
| `--spark-cbo` | `true` | |

### Hive (отдельный runner)

```bash
./scripts/hive/run-hive-factor.sh h0   # Tez, vec off
./scripts/hive/run-hive-factor.sh h1   # + vectorization
./scripts/hive/run-hive-factor.sh h2   # + CBO
./scripts/hive/run-hive-factor.sh h3   # LLAP cold
./scripts/hive/run-hive-factor.sh h4   # LLAP warm
```

Нужны `beeline` и (для h3/h4) Hive ≥ 2.0 с LLAP. DDL/SQL: [`scripts/hive/`](scripts/hive/).

## Ручной вызов одного mode

```bash
./scripts/submit-spark32.sh --num-executors 16 --executor-memory 8g -- \
  --mode=generate \
  --base-path="$BASE" \
  --layout-id=b0 \
  --target-size-tb=0.1 \
  --orc-bloom-filter-columns=none \
  --orc-sort-columns=none
```

## Ошибки аргументов

| Ситуация | Сообщение |
|---|---|
| Нет `--mode` | `Missing required argument: --mode=...` |
| Неверный формат | `Invalid argument: …. Use --key=value` |
| Неположительное число | `Argument --<key> must be positive` |
| `timestamp-end` ≤ start | `--timestamp-end must be greater than --timestamp-start` |
| `TARGET_SIZE_TB > 0.5` в factor-скриптах | отказ: max dataset 500 GB |

Подробности кластера, troubleshooting YARN/Hive и сбор логов — в [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md).
