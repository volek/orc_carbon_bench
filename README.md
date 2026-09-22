# orc-bench

Факторный бенчмарк **ORC + HDFS + Spark 3.2 (+ Hive/Tez/LLAP)** на штатном SDP-кластере.

Java 8 / Spark SQL приложение: `generate → validate → benchmark → report`.  
Кластер **не меняем** — `spark-submit` на YARN. CarbonData и BYOS Spark 3.1 вне scope.

| Документ | Содержание |
|---|---|
| [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md) | Пошаговый запуск на edge-ноде |
| [docs/cluster-run-protocol.md](docs/cluster-run-protocol.md) | **Порядок прогонов, очистка HDFS, проверки, выгрузка отчётов** |
| [docs/audei-st-workload-mapping.md](docs/audei-st-workload-mapping.md) | AUDEI/ST профиль → schema, suites, dual SLA |
| [docs/orc-hive-bench-plan.md](docs/orc-hive-bench-plan.md) | План реализации P0–P10 |
| [docs/ORC + HDFS + Spark + Hive benchmark.md](docs/ORC%20+%20HDFS%20+%20Spark%20+%20Hive%20benchmark.md) | Методика факторного эксперимента |

## Принципы

1. **Один фактор за раз** (partition / sort / bloom / stride / stripe / compression / file size / Spark toggles).
2. **Cold и warm не усреднять** (`--cache-state=cold|warm`).
3. Layout выбирают на Dataset **S/M**; финальный SLA — на Dataset **L**.
4. Hive читает **те же** ORC-файлы, что и Spark (`layouts/best_orc/orc`).
5. Один датасет ≤ **100 GB** (`--target-size-tb=0.1`). На HDFS ~**700 GB** свободно — лишние layout-копии всё равно удалять (много копий на S).

## Масштабы датасетов

| Имя | `--target-size-tb` | ≈ объём | Назначение |
|---|---|---|---|
| Smoke | `0.01` | ~10 GB | проверка пайплайна |
| **S** | `0.02` | ~20 GB | layout-факторы |
| **M** | `0.05` | ~50 GB | top-N подтверждение |
| **L** | `0.1` | **~100 GB (макс.)** | BEST_ORC, Spark S0/S1, Hive, concurrency, SLA |

Скрипты `run-factor.sh` / `run-layout-sweep.sh` / `run-sla-matrix.sh` отказывают при `TARGET_SIZE_TB > 0.1`.

## Требования и сборка

- Java 8, Hadoop / YARN / HDFS кластера
- Артефакт для submit: **`build/libs/orc-bench-all.jar`** (Shadow JAR; Spark/Hadoop — с кластера)
- Thin JAR `orc-bench-0.1.0-SNAPSHOT.jar` на кластер **не** копировать

```bash
./gradlew build    # или gradlew.bat build
./gradlew test
```

## Быстрый старт

Полный операционный протокол (порядок, HDFS-очистка, проверки, выгрузка отчётов):  
**[docs/cluster-run-protocol.md](docs/cluster-run-protocol.md)**.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=build/libs/orc-bench-all.jar   # на edge: ~/orc-bench/orc-bench-all.jar

# 1) Smoke
./scripts/run-smoke.sh

# 2) Baseline B0 на Dataset S
TARGET_SIZE_TB=0.02 SCENARIOS=audei ./scripts/run-factor.sh --layout=b0

# 3) AUDEI-priority layout sweep (partition/sort/bloom)
TARGET_SIZE_TB=0.02 SWEEP=audei ./scripts/run-layout-sweep.sh

# 4) Freeze BEST_ORC → Spark matrix → Hive → concurrency 9/18 → SLA на L
TARGET_SIZE_TB=0.02 SCENARIOS=audei ./scripts/run-factor.sh --layout=best_orc
./scripts/run-spark-exec-matrix.sh
SUITE=audei ./scripts/hive/run-hive-factor.sh h0
CONCURRENCY_LEVELS="9 18" SCENARIO=epk_eq_14d ./scripts/run-concurrency.sh
# очистить лишние layouts/*, затем:
TARGET_SIZE_TB=0.1 SCENARIOS=audei ./scripts/run-factor.sh --layout=best_orc
TARGET_SIZE_TB=0.1 ./scripts/run-sla-matrix.sh
```

Порядок R&D:

```text
smoke(audei) → B0 → layout sweep audei (S) → BEST_ORC → Spark S0/S1
      → Hive H0–H4 → concurrency 9/18 → SLA (L ≤ 100 GB)
```

## Скрипты

| Скрипт | Назначение |
|---|---|
| [`scripts/submit-spark32.sh`](scripts/submit-spark32.sh) | один YARN job (`--mode=…`); ресурсы до `--` |
| [`scripts/run-smoke.sh`](scripts/run-smoke.sh) | e2e smoke `0.01` (~10 GB), suite `audei` |
| [`scripts/run-factor.sh`](scripts/run-factor.sh) | **основной** фактор: generate→validate→benchmark→report (`SCENARIOS=audei`) |
| [`scripts/run-layout-sweep.sh`](scripts/run-layout-sweep.sh) | `SWEEP=audei` (partition/sort/bloom) или secondary |
| [`scripts/run-spark-exec-matrix.sh`](scripts/run-spark-exec-matrix.sh) | S0/S1 + toggles на BEST_ORC |
| [`scripts/hive/run-hive-factor.sh`](scripts/hive/run-hive-factor.sh) | Hive H0–H4; `SUITE=audei\|st\|doc` |
| [`scripts/run-concurrency.sh`](scripts/run-concurrency.sh) | **9 / 18** параллельных клиентов на `epk_eq_14d` |
| [`scripts/run-sla-matrix.sh`](scripts/run-sla-matrix.sh) | финальная SLA-матрица на L (`SCENARIOS=audei`) |

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
| `e1` / `e2` / `e3` | sort `epk_id` / `event_id` / `event_ts+epk_id` |
| `f0`–`f3`, `g005`/`g001`/`g0001` | bloom columns / FPP |
| `h5k`…`h50k`, `i64`…`i256` | stride / stripe |
| `jsnappy` / `jzstd` / `jzlib` | compression |
| `k32` / `k256` / `k1024` | target file size |
| `best_orc` | зафиксированный профиль победителей |
| `s0` / `s1` | Spark baseline OFF / optimized ON (читает `best_orc`) |

## Схема данных (AUDEI audit)

| Колонка | Card. | Роль |
|---|---|---|
| `epk_id` | высокая | ключ поиска / bloom / sort |
| `event_id` | высокая | id события |
| `event_ts` | — | время; Q6 |
| `name` | низкая | LOGON / FIND / LAUNCHER / ESA |
| `channel_type`, `state` | низкая | канал / успешность |
| `module`, `session`, `user_login` | средняя | |
| `payload_json` | — | JSON ~1.6 КБ; text_search / pruning |
| `event_year` / `month` / `day` / `hour` | — | partition prune |

Defaults: partition `event_year,event_month,event_day`, sort `epk_id`, bloom `epk_id`, `--avg-row-bytes=1600`.

## Query suite

| Сценарий | Документ | Что меряем |
|---|---|---|
| `partition_prune` | Q1 | partition pruning |
| `filter_high_cardinality` | Q2 | `epk_id` / `event_id` |
| `filter_medium_cardinality` | Q3 | `module` / `name` |
| `filter_low_cardinality` | Q4 | `channel_type` / `state` |
| `filter_in` | Q5 | `IN` |
| `filter_timestamp_range` | Q6 | range по `event_ts` |
| `projection` / `full_scan` | Q7 | column pruning |
| `group_by` / `group_by_heavy` | Q8 / Q9 | aggregation / AQE |
| `join_dictionary` | Q10 | JOIN name→family |
| `filter_log_format`, `filter_combined`, `text_search` | доп. | в `all`, не в `doc` |

`--benchmark-scenarios=doc` — Q1–Q10; `audei` — interactive SLA; `st` — archive ST mix; `all` — полный набор.

| Suite alias | Сценарии |
|---|---|
| `audei` | `epk_eq_1d`, `epk_eq_14d`, `epk_page`, `eq_filters`, `order_by_epk_day` |
| `st` | `no_filter`, `like_single`, `like_multi`, `like_fulltext`, `eq`, `in_list`, `rlike` |

## Метрики

Raw parquet / отчёт:

| Поле | Смысл |
|---|---|
| `duration_ms`, p50/p95/p99 | latency |
| `bytes_read`, `records_read`, `selectivity` | I/O / pruning |
| `layout_id`, `engine`, `cache_state` | фактор эксперимента |
| `scan_ratio` | `bytes_read / dataset_bytes` |
| `seconds_per_gb` | `duration_sec / (bytes_read / 1GiB)` (ST metric) |
| `sla_class` | `interactive` / `archive` / `legacy` |
| `query_category` | ST category (`7_EQ`, `3_LIKE_FULLTEXT`, …) |
| `duration_group` | `1_month` / `2_quarter` / … |
| `rows_cap_ok` | `rows_returned ≤ 2000` |
| `sla_ok` | по порогу класса: interactive 3000 ms, archive 120000 ms |

Markdown: Benchmark Summary, **SLA ≤ 3s**, Bloom comparison, Validation, Recommendations.

## Основные CLI-параметры

### Generate / ORC write

| Параметр | Дефолт | Описание |
|---|---|---|
| `--target-size-tb` | `0.1` | объём; **макс. 0.1** (~100 GB) на кластере |
| `--layout-id` | `default` | → `layouts/<id>/…` |
| `--partition-by` | `event_year,event_month,event_day` | или `none` |
| `--orc-sort-columns` | `epk_id` | `none` — unsorted |
| `--orc-bloom-filter-columns` | `epk_id` | или `none` |
| `--orc-bloom-filter-fpp` | `0.05` | FPP |
| `--orc-row-index-stride` | `10000` | ORC stride |
| `--orc-stripe-size-mb` | `64` | stripe |
| `--orc-compression` | `snappy` | `snappy` / `zstd` / `zlib` / `none` |
| `--target-file-size-mb` | `384` | целевой размер файла |
| `--avg-row-bytes` | `1600` | оценка строк / padding payload |
| `--seed` | `42` | воспроизводимость |

У **generate** и **validate** одинаковый `--orc-bloom-filter-columns`.

### Benchmark / Spark exec

| Параметр | Дефолт | Описание |
|---|---|---|
| `--benchmark-scenarios` | `all` | CSV / `all` / `doc` / `audei` / `st` |
| `--benchmark-warmup-runs` | `1` | прогрев |
| `--benchmark-repeat-runs` | `3` | измерения (≥3 для p50/p95) |
| `--clear-cache-between-runs` | `true` | cold protocol |
| `--cache-state` | `cold` | метка `cold` / `warm` |
| `--engine` | `spark` | `spark` / `hive_tez` / `hive_llap` |
| `--sla-threshold-ms` | `3000` | interactive SLA |
| `--archive-sla-threshold-ms` | `120000` | archive/ST SLA |
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
  --target-size-tb=0.02 \
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
| `TARGET_SIZE_TB > 0.1` в factor-скриптах / `--target-size-tb` | отказ: max dataset 100 GB |

Подробности кластера, troubleshooting YARN/Hive и сбор логов — в [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md).
