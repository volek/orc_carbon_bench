# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Кластер **не меняем**: используется штатный SDP Spark 3.2 (`spark-submit` на YARN).

Документ описывает **факторный эксперимент ORC + HDFS + Spark + Hive** после реализации plan P0–P10:

```text
smoke → B0 baseline → layout sweep (S) → BEST_ORC → Spark S0/S1
      → Hive H0–H4 → concurrency → SLA matrix (L)
```

Принципы:

1. **Один фактор за раз** (partition / sort / bloom / stride / …).
2. **Cold и warm не усреднять** (`--cache-state=cold|warm`).
3. Решения по layout — на Dataset **S/M**; финальный SLA — на Dataset **L**.
4. Hive читает **те же** ORC-файлы, что и Spark (`layouts/best_orc/orc`).
5. **Лимит объёма:** один датасет ≤ **500 GB** (`--target-size-tb=0.5`). На HDFS сейчас ~**700 GB** свободно — копий layout’ов держать минимум.

Канонический дизайн: [ORC + HDFS + Spark + Hive benchmark.md](ORC%20+%20HDFS%20+%20Spark%20+%20Hive%20benchmark.md).  
CLI-справка: [README.md](../README.md).

---

## 0. Масштабы датасетов и бюджет HDFS

| Имя | `--target-size-tb` | ≈ объём | Когда |
|---|---|---|---|
| Smoke | `0.01` | ~10 GB | первый прогон, проверка пайплайна |
| Dataset **S** | `0.1` | ~100 GB | все layout-факторы (по одному / с очисткой) |
| Dataset **M** | `0.25` | ~250 GB | спорные top-N layout’ов |
| Dataset **L** | `0.5` | **~500 GB (максимум)** | финал: BEST_ORC, S0/S1, Hive, concurrency, SLA |

| Ограничение | Значение |
|---|---|
| Макс. размер **одного** ORC-layout | **500 GB** (`0.5` ТБ) |
| Свободно на HDFS (ориентир) | **~700 GB** |
| Запас под reports / dictionary / временные файлы | оставляйте ≥150–200 GB |

**Бюджет места:** каждый layout — отдельная копия. При S≈100 GB пять layout’ов ≈ 500 GB; на L=500 GB перед generate удалите лишние `layouts/*`, кроме того что нужно для сравнения.

```bash
# Пример очистки проигравших layout’ов перед Dataset L
hdfs dfs -du -h -s "$BASE"/layouts/*
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/d0 "$BASE"/layouts/e2 ...
```

Не делайте выводы по performance только на smoke (`0.01`).  
**Не** указывайте `--target-size-tb` > `0.5` на этом кластере.

---

## 1. Параметры кластера

| Параметр | Значение |
|---|---|
| HDFS namenode | `dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| HDFS URI | `hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| Spark кластера | `3.2.1.3.5.7.0-1-SNAPSHOT` |
| Scala | `2.12.x` |
| JVM | OpenJDK `1.8.0_472` |
| Hadoop | `3.1.3.3.5.7.0-1-SNAPSHOT` |
| Артефакт | `orc-bench-all.jar` |
| BASE (smoke / factor) | `hdfs:///user/hdfs_migration_user/orc_test` |
| BASE (legacy bloom A/B) | `hdfs:///user/hdfs_migration_user/orc_test_pilot` |

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
# опционально:
export NUM_EXECUTORS=16
export EXECUTOR_MEMORY=8g
export EXECUTOR_CORES=4
export DRIVER_MEMORY=4g
```

Пути `hdfs:///...` используют default FS из `core-site.xml`.

---

## 2. Подготовка на edge-ноде

### 2.1. Сборка и копирование

На машине разработки:

```bash
./gradlew build
# артефакт: build/libs/orc-bench-all.jar
```

На edge:

```bash
scp build/libs/orc-bench-all.jar user@edge-host:~/orc-bench/
scp -r scripts docs README.md user@edge-host:~/orc-bench/
```

### 2.2. Проверка окружения

```bash
cd ~/orc-bench
java -version          # ожидается 1.8.x
spark-submit --version # кластерный Spark 3.2.1.x
hdfs dfs -ls "$BASE" || hdfs dfs -mkdir -p "$BASE"

sed -i 's/\r$//' scripts/*.sh scripts/hive/*.sh
chmod +x scripts/*.sh scripts/hive/*.sh
```

### 2.3. Ресурсы YARN / `submit-spark32.sh`

Низкоуровневый запуск **всегда** через [`scripts/submit-spark32.sh`](../scripts/submit-spark32.sh).

| Параметр spark-submit | Env | Дефолт | Смысл |
|---|---|---|---|
| `--num-executors` | `NUM_EXECUTORS` | `16` | YARN executors |
| `--executor-memory` | `EXECUTOR_MEMORY` | `8g` | память executor |
| `--executor-cores` | `EXECUTOR_CORES` | `4` | ядра executor |
| `--driver-memory` | `DRIVER_MEMORY` | `4g` | память driver |
| `spark.yarn.am.memory` | = `DRIVER_MEMORY` | `4g` | AM в cluster mode |
| `spark.yarn.am.memoryOverhead` | `YARN_AM_MEMORY_OVERHEAD` | `512m` | overhead AM |

Приоритет: **флаг до `--`** > **env** > **дефолт скрипта**.

```bash
# Вариант A — дефолты
./scripts/submit-spark32.sh -- --mode=benchmark --base-path="$BASE"

# Вариант B — env
export EXECUTOR_MEMORY=16g DRIVER_MEMORY=8g
./scripts/run-factor.sh --layout=b0

# Вариант C — флаги до --
./scripts/submit-spark32.sh \
  --num-executors 16 --executor-memory 16g --driver-memory 8g -- \
  --mode=generate --base-path="$BASE" --layout-id=b0 --target-size-tb=0.01 \
  --orc-bloom-filter-columns=none
```

Скрипт по умолчанию отключает Hive/HBase credentials (иначе submit может зависнуть на Metastore). Для **Spark ORC** это нормально. Для Hive-бенчмарка используйте Beeline (`scripts/hive/`), не `submit-spark32.sh`.

Типичные профили:

| Профиль | Workers | Executor memory | Когда |
|---|---|---|---|
| Smoke | 16 | 8g | `0.01` ТБ (~10 GB) |
| Dataset S / M | 16 | 8–16g | `0.1`–`0.25` ТБ |
| Dataset L (max 500 GB) | 16+ | 16g+ | `0.5` ТБ, при OOM — больше памяти |

В stderr submit пишет: `spark-submit resources: num-executors=...` — сверьте с квотой очереди.

---

## 3. Структура HDFS и режимы приложения

### 3.1. Режимы (`--mode`)

| Режим | Что делает | Выход |
|---|---|---|
| `generate` | синтетика → ORC (+ dictionary для JOIN) | `<orc-path>/`, `<dictionary-path>/` |
| `validate` | качество + bloom metadata | `<reports>/raw/validation/` |
| `benchmark` | query suite, метрики, SLA | `<reports>/raw/benchmark/` |
| `report` | агрегация → summary | `<reports>/summary/` |

### 3.2. Пути с `--layout-id`

При `--layout-id=default` (или без флага) — legacy-пути:

```text
$BASE/orc/
$BASE/dictionary/
$BASE/reports/raw/benchmark|validation/
$BASE/reports/summary/
```

При `--layout-id=<id>` (рекомендуется для факторов):

```text
$BASE/layouts/<id>/orc/
$BASE/layouts/<id>/dictionary/
$BASE/layouts/<id>/reports/raw/benchmark/
$BASE/layouts/<id>/reports/raw/validation/
$BASE/layouts/<id>/reports/summary/
```

Пример: `--layout-id=b0` → `hdfs:///…/orc_test/layouts/b0/orc`.

Переопределение вручную: `--orc-path`, `--dictionary-path`, `--reports-path`, `--reports-benchmark-path`, `--reports-validation-path`.

### 3.3. Метрики в raw benchmark

Помимо `duration_ms`, `bytes_read`, `records_read`, `selectivity`:

| Поле | Смысл |
|---|---|
| `layout_id` | id layout’а |
| `engine` | `spark` / `hive_tez` / `hive_llap` |
| `cache_state` | `cold` / `warm` |
| `scan_ratio` | `bytes_read / dataset_bytes` |
| `sla_ok` | `duration_ms ≤ sla-threshold-ms` (дефолт 3000) |
| `dataset_label` | метка прогона (часто = layout id) |

В markdown-отчёте: **Benchmark Summary**, **SLA ≤ 3s**, **Bloom filter comparison**, **Validation**, **Recommendations**.

---

## 4. Скрипты: что чем запускать

| Скрипт | Назначение | Типичный объём |
|---|---|---|
| [`submit-spark32.sh`](../scripts/submit-spark32.sh) | один `--mode=…` | любой |
| [`run-smoke.sh`](../scripts/run-smoke.sh) | быстрый e2e (legacy пути, bloom ON по дефолту app) | `0.01` |
| [`run-factor.sh`](../scripts/run-factor.sh) | **основной** фактор: generate→validate→benchmark→report для одного layout | `0.01`–`0.5` (макс.) |
| [`run-layout-sweep.sh`](../scripts/run-layout-sweep.sh) | пачка layout’ов (P3–P6); следите за местом на HDFS | `0.1` (S) |
| [`run-spark-exec-matrix.sh`](../scripts/run-spark-exec-matrix.sh) | S0/S1 + toggles pushdown/AQE/… на BEST_ORC | без generate |
| [`run-bloom-ab.sh`](../scripts/run-bloom-ab.sh) | legacy bloom A/B (orc vs orc_bloom) | `0.1` |
| [`run-bench-pipeline.sh`](../scripts/run-bench-pipeline.sh) | validate→benchmark→report без generate | — |
| [`hive/run-hive-factor.sh`](../scripts/hive/run-hive-factor.sh) | Hive/Tez/LLAP H0–H4 на тех же ORC | после BEST_ORC |
| [`run-concurrency.sh`](../scripts/run-concurrency.sh) | параллельные клиенты 1/5/10/25/50 | BEST_ORC |
| [`run-sla-matrix.sh`](../scripts/run-sla-matrix.sh) | финальный SLA Spark+Hive | Dataset L |

Рекомендуемый порядок:

```text
1. run-smoke.sh                         # инфраструктура OK?
2. run-factor.sh --layout=b0            # baseline T0
3. run-layout-sweep.sh                  # факторы на S
4. выбрать победителей → best_orc
5. run-factor.sh --layout=best_orc
6. run-spark-exec-matrix.sh             # S0/S1
7. hive/run-hive-factor.sh h0…h4
8. run-concurrency.sh
9. run-sla-matrix.sh                    # на L
```

---

## 5. Smoke (первый прогон)

Проверяет YARN, HDFS, JAR, полный пайплайн. **Не** используйте для выводов по layout/SLA.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar

./scripts/run-smoke.sh
# эквивалент: TARGET_SIZE_TB=0.01, generate→validate→benchmark→report
```

Критерий успеха:

- все job `SUCCEEDED`
- есть `$BASE/orc`, `$BASE/reports/raw/{benchmark,validation}`, `$BASE/reports/summary/`
- Validation PASS
- в Benchmark Summary есть `avg_bytes_read`, `runs ≥ 3`

Альтернатива — smoke через factor (пути под `layouts/b0`):

```bash
TARGET_SIZE_TB=0.01 BENCHMARK_REPEAT_RUNS=3 BENCHMARK_WARMUP_RUNS=1 \
  ./scripts/run-factor.sh --layout=b0
```

---

## 6. Factor runner — основной способ запуска

[`scripts/run-factor.sh`](../scripts/run-factor.sh) для **одного** layout:

```bash
./scripts/run-factor.sh --layout=b0
# или
LAYOUT=d1 TARGET_SIZE_TB=0.1 CACHE_STATE=cold ./scripts/run-factor.sh
```

### 6.1. Переменные окружения

| Env | Дефолт | Описание |
|---|---|---|
| `BASE` | `…/orc_test` | корень эксперимента |
| `SEED` | `42` | generate / фильтры |
| `TARGET_SIZE_TB` | `0.01` | объём в ТБ; S=`0.1`, M=`0.25`, L=`0.5` (не больше) |
| `BENCHMARK_WARMUP_RUNS` | `3` | прогрев |
| `BENCHMARK_REPEAT_RUNS` | `5` | измеряемые повторы |
| `CACHE_STATE` | `cold` | метка cold/warm |
| `ENGINE` | `spark` | метка engine в метриках |
| `SLA_THRESHOLD_MS` | `3000` | порог `sla_ok` |
| `SCENARIOS` | `doc` | suite Q1–Q10; `all` — все сценарии |
| `SKIP_GENERATE` | `0` | `1` — только validate/benchmark |
| `SKIP_VALIDATE` | `0` | пропуск validate |
| `CLEAR_CACHE` | `true` | cold read между runs |

Флаги скрипта: `--layout=…`, `--skip-generate`, `--skip-validate`, плюс любые `--key=value` приложения.

### 6.2. Каталог layout’ов

| Layout | Что меняет | Этап плана |
|---|---|---|
| `b0` | ORC baseline: без bloom, без sort, default partition | P1 |
| `d0` | `partition-by=none` | P3 |
| `d1` | date partitions only (`year,month,day`) | P3 |
| `d2` | + `event_hour` | P3 |
| `e1` | sort `event_id` | P4 |
| `e2` | sort `product_id` | P4 |
| `e3` | sort `timestamp,event_id` | P4 |
| `f0` | bloom off | P5 |
| `f1` | bloom high (`event_id,user_id`) | P5 |
| `f2` | bloom medium | P5 |
| `f3` | bloom high+medium+low | P5 |
| `g005` / `g001` / `g0001` | FPP 0.05 / 0.01 / 0.001 | P5 |
| `h5k` / `h10k` / `h20k` / `h50k` | row index stride | P6 |
| `i64` / `i128` / `i256` | stripe MB | P6 |
| `jsnappy` / `jzstd` / `jzlib` | compression | P6 |
| `k32` / `k256` / `k1024` | target file size MB | P6 |
| `best_orc` | зафиксированный профиль победителей | после P6 |
| `s0` | Spark baseline toggles OFF, читает `best_orc` | P7 |
| `s1` | Spark optimized toggles ON, читает `best_orc` | P7 |

После прогона:

```text
$BASE/layouts/<layout>/orc/
$BASE/layouts/<layout>/dictionary/
$BASE/layouts/<layout>/reports/summary/factor-<layout>-<cache>.md
```

Логи на edge: `factor-<layout>-generate.log`, `…-benchmark-cold.log`, `…-report.log`.

### 6.3. Пример: baseline B0 на Dataset S

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export NUM_EXECUTORS=16
export EXECUTOR_MEMORY=8g

TARGET_SIZE_TB=0.1 \
BENCHMARK_WARMUP_RUNS=3 \
BENCHMARK_REPEAT_RUNS=5 \
CACHE_STATE=cold \
SCENARIOS=doc \
  ./scripts/run-factor.sh --layout=b0
```

Что внутри (упрощённо):

1. `generate` с `--layout-id=b0 --orc-bloom-filter-columns=none --orc-sort-columns=none …`
2. `validate` с теми же ORC/bloom-флагами
3. `benchmark` с `--benchmark-scenarios=doc --cache-state=cold --sla-threshold-ms=3000`
4. `report` → `factor-b0-cold.md`

### 6.4. Ручной эквивалент одного шага

```bash
./scripts/submit-spark32.sh -- \
  --mode=generate \
  --base-path="$BASE" \
  --layout-id=b0 \
  --target-size-tb=0.1 \
  --seed=42 \
  --orc-bloom-filter-columns=none \
  --orc-sort-columns=none \
  --orc-row-index-stride=10000 \
  --partition-by=event_year,event_month,event_day,log_format \
  --orc-compression=snappy \
  --orc-stripe-size-mb=64
```

**Важно:** у `generate` и `validate` одинаковые `--orc-bloom-filter-columns` (иначе check `orc_bloom_filters` падает).

---

## 7. Layout sweep (P3–P6) на Dataset S

При ~700 GB свободно и S≈100 GB не гоняйте `SWEEP=all` без очистки: каждый layout — полная копия. Предпочтительно **по группам** и удалять проигравшие варианты после выбора победителя группы.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export TARGET_SIZE_TB=0.1
export CACHE_STATE=cold

hdfs dfs -df -h   # убедиться, что свободно ≳ 200–300 GB на группу

# по группам (рекомендуется):
SWEEP=baseline   ./scripts/run-layout-sweep.sh   # b0
SWEEP=partition  ./scripts/run-layout-sweep.sh   # d0 d1 d2
# выбрать D*, удалить остальные partition-layout’ы, затем:
SWEEP=sort       ./scripts/run-layout-sweep.sh   # e1 e2 e3
SWEEP=bloom      ./scripts/run-layout-sweep.sh   # f0 f1 f2 f3
SWEEP=fpp        ./scripts/run-layout-sweep.sh
SWEEP=stride     ./scripts/run-layout-sweep.sh
SWEEP=stripe     ./scripts/run-layout-sweep.sh
SWEEP=compression ./scripts/run-layout-sweep.sh
SWEEP=files      ./scripts/run-layout-sweep.sh

# SWEEP=all — только если места хватает и старые layout’ы уже снесены
```

В конце sweep пишет сводный report `layout-sweep-<SWEEP>.md` (discovery по `layouts/*/reports`).

На Dataset **M** (`TARGET_SIZE_TB=0.25`) гоняйте только 2–3 кандидата в BEST_ORC, не весь sweep.

### Как выбрать BEST_ORC

1. Смотрите p50/p95, `avg_bytes_read`, `avg_scan_ratio`, `sla_success` **по одному фактору**.
2. Зафиксируйте победителей (пример): `d1` + `e1` + `f1` + `g001` + `h10k` + `jsnappy` + `k256`.
3. Профиль `best_orc` в `run-factor.sh` уже задан как разумный default — **подправьте** `apply_layout best_orc` в скрипте под ваши победители, либо передайте явные флаги:

```bash
TARGET_SIZE_TB=0.1 ./scripts/run-factor.sh --layout=best_orc \
  --partition-by=event_year,event_month,event_day \
  --orc-sort-columns=event_id \
  --orc-bloom-filter-columns=event_id,user_id \
  --orc-bloom-filter-fpp=0.01 \
  --orc-row-index-stride=10000 \
  --orc-compression=snappy \
  --target-file-size-mb=384
```

После freeze **не перегенерируйте** `best_orc` без нужды — на нём крутятся Spark S0/S1 и Hive.

---

## 8. Spark execution matrix (P7) — те же файлы, другие конфиги

Требует уже существующий `$BASE/layouts/best_orc/orc`.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export TARGET_SIZE_TB=0.1   # только для dataset-bytes / scan_ratio
export CACHE_STATE=cold

./scripts/run-spark-exec-matrix.sh
```

Что запускается:

1. **S0** — pushdown/vectorized/AQE/DPP/CBO = OFF (без generate)
2. **S1** — все ON
3. One-factor toggles: `c0/c1` pushdown, `l0/l1` vectorized, `m0/m1` AQE, `n0/n1` DPP, `o0/o1` CBO

Ручной S1 warm:

```bash
CACHE_STATE=warm SKIP_GENERATE=1 SKIP_VALIDATE=1 \
  ./scripts/run-factor.sh --layout=s1
```

Флаги приложения (можно и без скрипта):

| Параметр | Дефолт | Эффект |
|---|---|---|
| `--spark-orc-filter-pushdown` | `true` | predicate pushdown |
| `--spark-orc-vectorized` | `true` | vectorized reader |
| `--spark-aqe` | `true` | AQE |
| `--spark-dpp` | `true` | dynamic partition pruning |
| `--spark-cbo` | `true` | CBO |

Cold protocol: `--clear-cache-between-runs=true` + `--cache-state=cold`.  
Warm: `CLEAR_CACHE=false` + `--cache-state=warm` (не смешивать в одном среднем).

---

## 9. Query suite (сценарии)

| Сценарий | Документ | Что проверяет |
|---|---|---|
| `partition_prune` | Q1 | pruning по `event_year/month/day` |
| `filter_high_cardinality` | Q2 | point lookup / bloom / min-max |
| `filter_medium_cardinality` | Q3 | medium card |
| `filter_low_cardinality` | Q4 | low card (bloom часто слаб) |
| `filter_in` | Q5 | `IN` / bloom |
| `filter_timestamp_range` | Q6 | range / min-max |
| `projection` / `full_scan` | Q7 | column pruning |
| `group_by` | Q8 | aggregation |
| `group_by_heavy` | Q9 | тяжёлый GROUP BY |
| `join_dictionary` | Q10 | JOIN с `$…/dictionary` |
| `filter_log_format`, `filter_combined`, `text_search` | доп. | в `all`, не в `doc` |

```bash
# только document suite
SCENARIOS=doc ./scripts/run-factor.sh --layout=b0

# один сценарий
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --layout-id=best_orc \
  --benchmark-scenarios=filter_high_cardinality \
  --benchmark-repeat-runs=10 --cache-state=cold
```

---

## 10. Hive / Tez / LLAP (P8)

На **тех же** файлах `layouts/best_orc/orc` (+ dictionary).

### 10.1. Требования

- `beeline` на edge
- Hive ≥ 2.0 для LLAP (`h3`/`h4`)
- при необходимости: `export HIVE_JDBC_URL='jdbc:hive2://…'`

`submit-spark32.sh` **не** используется для Hive-queries (credentials Hive намеренно выключены).

### 10.2. Профили

| Профиль | Engine | Настройки |
|---|---|---|
| `h0` | hive_tez | vectorization OFF, CBO OFF, LLAP none |
| `h1` | hive_tez | vectorization ON |
| `h2` | hive_tez | vectorization + CBO |
| `h3` | hive_llap | LLAP **cold** |
| `h4` | hive_llap | LLAP **warm** |

### 10.3. Запуск

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export LAYOUT=best_orc

# DDL + Q1–Q10 для профиля
./scripts/hive/run-hive-factor.sh h0
./scripts/hive/run-hive-factor.sh h1
./scripts/hive/run-hive-factor.sh h2
./scripts/hive/run-hive-factor.sh h3   # cold: при необходимости рестарт LLAP cache до прогона
./scripts/hive/run-hive-factor.sh h4   # warm: повторные запросы на горячем cache
```

Фильтры (если sample из Spark другой):

```bash
export FILTER_Y=2024 FILTER_M=6 FILTER_D=15
export FILTER_EVENT_ID='evt-…' FILTER_USER_ID=123
./scripts/hive/run-hive-factor.sh h2
```

Скрипт:

1. Создаёт external tables (`scripts/hive/ddl_external_orc.sql`)
2. Прогоняет сценарии с warmup/repeats
3. Пишет CSV таймингов; при наличии `hdfs` — кладёт в  
   `$BASE/layouts/best_orc/reports/raw/benchmark_hive_<profile>/`
4. Пытается ingest в parquet через [`ingest-hive-csv.sh`](../scripts/hive/ingest-hive-csv.sh)

SQL-эталоны: [`scripts/hive/queries_q1_q10.sql`](../scripts/hive/queries_q1_q10.sql).

### 10.4. LLAP cold vs warm

- **Cold (`h3`)**: очистить / перезапустить LLAP ORC cache (операция админа), затем один измеряемый прогон.
- **Warm (`h4`)**: те же queries без сброса cache; смотреть стабильность p95/SLA.

Не усредняйте h3 и h4.

---

## 11. Concurrency (P9)

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export LAYOUT=best_orc
export SCENARIO=filter_high_cardinality
export CACHE_STATE=warm

# Spark
ENGINE=spark CONCURRENCY_LEVELS="1 5 10 25 50" ./scripts/run-concurrency.sh

# Hive LLAP warm
ENGINE=hive_llap HIVE_PROFILE=h4 CONCURRENCY_LEVELS="1 5 10" ./scripts/run-concurrency.sh
```

Результат: `result/concurrency/concurrency-<engine>.csv`  
колонки: `engine,concurrency,scenario,avg_ms,p50_ms,p95_ms,p99_ms,sla_success`.

На большой конкуренции уменьшите `NUM_EXECUTORS` у параллельных Spark-клиентов или снизьте уровни — иначе очередь YARN раздует latency.

---

## 12. Финальная SLA-матрица (P10)

На Dataset **L** = **0.5 ТБ (~500 GB, максимум)** после готовности BEST_ORC на S/M:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export TARGET_SIZE_TB=0.5
export EXECUTOR_MEMORY=16g

# Перед L: освободить место (нужно ~500 GB + запас; свободно ориентир ~700 GB)
hdfs dfs -df -h
hdfs dfs -du -h -s "$BASE"/layouts/*
# удалить всё, кроме нужного; затем один generate BEST_ORC на L:
TARGET_SIZE_TB=0.5 ./scripts/run-factor.sh --layout=best_orc

./scripts/run-sla-matrix.sh
```

Скрипт:

1. Spark S1 cold + warm (без generate)
2. Hive h0–h4
3. Сводный `report` → `sla-matrix-final`

Критерий (ориентир документа): для целевых interactive queries  
`p95 ≲ 3000 ms`, `sla_success ≳ 98%`, cold и warm смотреть **отдельно**.

---

## 13. Legacy Bloom A/B (опционально)

Старый путь без `layouts/` — по-прежнему валиден для быстрого сравнения bloom ON/OFF:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
TARGET_SIZE_TB=0.1 BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bloom-ab.sh
```

Пишет `$BASE/orc` + `$BASE/orc_bloom` и `bloom-ab-report.md`.  
Для полного факторного R&D предпочтительнее `run-factor.sh` / `SWEEP=bloom`.

---

## 14. Полный справочник параметров приложения

Формат: **`--key=value`** (обязательно `=`).

### Общие / пути / эксперимент

| Параметр | Дефолт | Описание |
|---|---|---|
| `--mode` | — | `generate` / `validate` / `benchmark` / `report` |
| `--base-path` | `…/orc_test` | корень |
| `--layout-id` | `default` | layout; ≠default → `layouts/<id>/…` |
| `--orc-path` | auto | путь ORC |
| `--dictionary-path` | auto | dictionary для Q10 |
| `--reports-path` | auto | корень отчётов |
| `--seed` | `42` | воспроизводимость |
| `--engine` | `spark` | метка в метриках |
| `--cache-state` | `cold` | `cold` / `warm` |
| `--sla-threshold-ms` | `3000` | порог SLA |
| `--dataset-bytes` | из `target-size-tb` | знаменатель scan_ratio |

### Generate / ORC write

| Параметр | Дефолт | Описание |
|---|---|---|
| `--target-size-tb` | `0.5` | объём в ТБ; **макс. 0.5** (~500 GB) на этом кластере |
| `--partition-by` | `event_year,event_month,event_day,log_format` | или `none` |
| `--orc-sort-columns` | `none` | sortWithinPartitions |
| `--orc-bloom-filter-columns` | high+medium ids | или `none` |
| `--orc-bloom-filter-fpp` | `0.05` | FPP |
| `--orc-row-index-stride` | `10000` | ORC stride |
| `--orc-stripe-size-mb` | `64` | stripe |
| `--orc-compression` | `snappy` | `snappy`/`zstd`/`zlib`/`none` |
| `--target-file-size-mb` | `384` | целевой размер файла |

### Benchmark / Spark exec

| Параметр | Дефолт | Описание |
|---|---|---|
| `--benchmark-scenarios` | `all` | CSV, `all`, или `doc` |
| `--benchmark-warmup-runs` | `1` | прогрев |
| `--benchmark-repeat-runs` | `3` | измерения |
| `--clear-cache-between-runs` | `true` | cold protocol |
| `--benchmark-dataset-label` | auto | метка в отчёте |
| `--spark-orc-filter-pushdown` | `true` | |
| `--spark-orc-vectorized` | `true` | |
| `--spark-aqe` | `true` | |
| `--spark-dpp` | `true` | |
| `--spark-cbo` | `true` | |

### Validate

Generate и validate: **одинаковый** `--orc-bloom-filter-columns`.  
`all` включает `orc_bloom_filters` (present/absent).

### Report

| Параметр | Дефолт |
|---|---|
| `--report-name` | `benchmark-report` |
| `--report-formats` | `parquet,csv,json,markdown` |

Report ищет parquet под `reports/raw/` и рекурсивно под `layouts/*/reports/…`.

---

## 15. Проверки после прогона

```bash
hdfs dfs -du -h -s "$BASE"/layouts/*/orc "$BASE"/reports 2>/dev/null
hdfs dfs -ls -R "$BASE"/layouts/b0/reports/summary | head
hdfs dfs -cat "$BASE"/layouts/b0/reports/summary/factor-b0-cold.md | head -80

yarn application -list -appStates FINISHED | head
yarn logs -applicationId application_XXXXXXXX_XXXX > yarn-app.log
```

Checklist адекватного прогона на S:

| Проверка | Ожидание |
|---|---|
| Объём | `TARGET_SIZE_TB ≥ 0.1` |
| `runs` | ≥ 3 (лучше 5) |
| Validation | PASS (bloom present/absent согласован) |
| Cold/warm | отдельные файлы/метки, не смешаны |
| Selective vs full_scan | меньший `avg_bytes_read` на фильтрах |
| SLA section | есть `sla_success` / `sla_ok` |

---

## 16. Сбор логов для анализа

```bash
APP_ID=application_XXXXXXXX_XXXX
mkdir -p ~/bench-logs && cd ~/bench-logs

cp ~/orc-bench/smoke-*.log . 2>/dev/null || true
cp ~/orc-bench/factor-*.log . 2>/dev/null || true
cp ~/orc-bench/layout-sweep-*.log . 2>/dev/null || true
cp ~/orc-bench/bloom-ab-*.log . 2>/dev/null || true
cp ~/orc-bench/sla-matrix-*.log . 2>/dev/null || true
yarn logs -applicationId "$APP_ID" > yarn-${APP_ID}.log 2>&1

hdfs dfs -du -h -s "$BASE"/* > hdfs-du.txt 2>&1
hdfs dfs -get "$BASE"/layouts ./layouts-copy 2>&1 || true
hdfs dfs -get "$BASE"/reports/summary ./summary 2>&1 || true

{ java -version; spark-submit --version; hadoop version; } > env-versions.txt 2>&1
tar -czf bench-logs.tgz *.log hdfs-*.txt env-versions.txt layouts-copy summary 2>/dev/null
```

Минимум при падении: полная команда + exit code + `yarn logs` + совпадение bloom-флагов generate/validate.

---

## 17. Типичные ошибки

### 17.1. YARN RM `Connection refused` на `:8032`

```bash
yarn node -list
yarn rmadmin -getAllServiceState
```

### 17.2. SSL `sdp-deployer`

Платформенный `spark.ssl.*` — править в Ambari, не в приложении.

### 17.3. Submit зависает на Hive/HBase tokens

`submit-spark32.sh` уже ставит:

```text
spark.security.credentials.hive.enabled=false
spark.security.credentials.hbase.enabled=false
```

Если вызываете `spark-submit` вручную — добавьте те же conf.

### 17.4. AM `exitCode: 13` / слишком большой AM

Без явного `spark.yarn.am.memory` Spark может взять ≈ executor-memory. Используйте `submit-spark32.sh` или задайте `DRIVER_MEMORY`.

### 17.5. Validation `orc_bloom_filters` FAIL

Несовпадение `--orc-bloom-filter-columns` у generate и validate.  
Для `b0`/`f0` оба раза должно быть `none`.

### 17.6. `join_dictionary` пустой / ошибка чтения

Нет `$…/dictionary` — нужен generate с новым JAR (dictionary пишется в конце generate).

### 17.7. Report «No report input data»

Не было успешных validate/benchmark, или смотрите не тот `--base-path` / `--layout-id`.  
Сводный report с корня: `--base-path=$BASE` без layout (подхватит `layouts/*/…`).

### 17.8. Hive LLAP недоступен

Профили `h3`/`h4` пометить N/A; продолжайте H0–H2 и Spark S0/S1. Минимальная версия Hive для LLAP — 2.0; фактическая — из SDP.

---

## 18. Краткая шпаргалка команд

```bash
# 0) env
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export NUM_EXECUTORS=16 EXECUTOR_MEMORY=8g

# 1) smoke
./scripts/run-smoke.sh

# 2) baseline + sweep на S
TARGET_SIZE_TB=0.1 ./scripts/run-factor.sh --layout=b0
TARGET_SIZE_TB=0.1 ./scripts/run-layout-sweep.sh

# 3) freeze + Spark matrix
TARGET_SIZE_TB=0.1 ./scripts/run-factor.sh --layout=best_orc
./scripts/run-spark-exec-matrix.sh

# 4) Hive
./scripts/hive/run-hive-factor.sh h0
./scripts/hive/run-hive-factor.sh h4

# 5) concurrency + SLA на L (макс. 500 GB); сначала очистить лишние layouts
ENGINE=spark CONCURRENCY_LEVELS="1 5 10" ./scripts/run-concurrency.sh
TARGET_SIZE_TB=0.5 EXECUTOR_MEMORY=16g ./scripts/run-factor.sh --layout=best_orc
TARGET_SIZE_TB=0.5 EXECUTOR_MEMORY=16g ./scripts/run-sla-matrix.sh
```
