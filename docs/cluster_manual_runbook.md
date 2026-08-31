# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Порядок: **smoke (`0.01`) → Pilot / адекватная картина (`≥0.1`)** → при необходимости **Bloom A/B** или Full.  
Кластер **не меняем**: используется штатный SDP Spark 3.2.

## Параметры кластера

| Параметр | Значение |
|---|---|
| HDFS namenode | `dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| HDFS URI | `hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| Spark кластера | `3.2.1.3.5.7.0-1-SNAPSHOT` |
| Scala | `2.12.x` |
| JVM | OpenJDK `1.8.0_472` |
| Hadoop | `3.1.3.3.5.7.0-1-SNAPSHOT` |
| Артефакт | `orc-bench-all.jar` |
| BASE | `hdfs:///user/hdfs_migration_user/orc_test` |

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
```

Путь использует default FS из `core-site.xml` (`hdfs:///...`).

---

## 1. Подготовка на edge-ноде

Скопируйте fat JAR с машины сборки:

```bash
scp build/libs/orc-bench-all.jar user@edge-host:~/orc-bench/
scp -r scripts user@edge-host:~/orc-bench/
```

На edge:

```bash
cd ~/orc-bench
java -version          # ожидается 1.8.x
spark-submit --version # кластерный Spark 3.2.1.x
hdfs dfs -ls "$BASE" || hdfs dfs -mkdir -p "$BASE"

sed -i 's/\r$//' scripts/*.sh   # если скрипты приехали с Windows CRLF
chmod +x scripts/*.sh
```

При известных квотах YARN настройте ресурсы spark-submit (см. ниже).

### Ресурсы YARN / spark-submit

`scripts/submit-spark32.sh` всегда выставляет ресурсы для workers (executors), если вы не передали те же флаги явно.

| Параметр spark-submit | Env-переменная | Дефолт | Смысл |
|---|---|---|---|
| `--num-executors` | `NUM_EXECUTORS` | `16` | Число workers (YARN executors) |
| `--executor-memory` | `EXECUTOR_MEMORY` | `8g` | Память одного worker |
| `--executor-cores` | `EXECUTOR_CORES` | `4` | Ядра на worker |
| `--driver-memory` | `DRIVER_MEMORY` | `4g` | Память driver |
| `spark.yarn.am.memory` | `DRIVER_MEMORY` | `4g` | AM в cluster mode (явно, не от executor) |
| `spark.yarn.am.memoryOverhead` | `YARN_AM_MEMORY_OVERHEAD` | `512m` | Overhead AM-контейнера |

Приоритет: **явный флаг до `--`** > **env** > **дефолт скрипта**.  
`run-smoke.sh`, `run-bench-pipeline.sh` и `run-bloom-ab.sh` вызывают `submit-spark32.sh` без своих spark-флагов — наследуют эти дефолты (или ваш `export`).

Без явного `spark.yarn.am.memory` Spark 3.2 в cluster mode может выделить AM ≈ `executor-memory` (при `8g` → ~9011 MB), что на кластере с лимитом 9216 MB/container приводит к `exitCode: 13` на launch AM.

**Вариант A — дефолты скрипта (16 workers × 8g):**

```bash
./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

**Вариант B — через env (удобно для smoke/pipeline):**

```bash
export NUM_EXECUTORS=16
export EXECUTOR_MEMORY=16g   # больше памяти на worker
export EXECUTOR_CORES=4
export DRIVER_MEMORY=8g

./scripts/run-smoke.sh
# или один шаг:
./scripts/submit-spark32.sh -- --mode=benchmark --base-path="$BASE"
```

**Вариант C — флаги spark-submit до `--`:**

```bash
./scripts/submit-spark32.sh \
  --num-executors 16 \
  --executor-memory 16g \
  --executor-cores 4 \
  --driver-memory 8g \
  -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

Типичные профили:

| Профиль | Workers | Executor memory | Когда |
|---|---|---|---|
| Smoke / default | 16 | 8g | обычный smoke `0.01` ТБ |
| Тяжёлый generate / pilot | 16 | 16g | мало памяти на executor, OOM, большой `--target-size-tb` |
| Узкая квота YARN | 8 | 4g | очередь режет контейнеры |

Перед submit скрипт пишет в stderr строку вида  
`spark-submit resources: num-executors=... executor-memory=...` — сверьте с квотой очереди.

---

## 2. Справочник: скрипты, режимы и параметры

### Общая схема

```text
generate → validate → benchmark → report
```

Каждый шаг — отдельный `spark-submit` (отдельное YARN-приложение).  
Низкий уровень: всегда `scripts/submit-spark32.sh`.  
Высокий уровень: обёртки `run-*.sh` для типовых пайплайнов.

### Скрипты-обёртки

| Скрипт | Назначение | Шаги | Дефолт `TARGET_SIZE_TB` | Когда использовать |
|---|---|---|---|---|
| [`submit-spark32.sh`](../scripts/submit-spark32.sh) | один `--mode=...` | 1 job | — | ручной запуск, отладка одного шага |
| [`run-smoke.sh`](../scripts/run-smoke.sh) | smoke пайплайн | generate → validate → benchmark → report | `0.01` | **первый** прогон на кластере |
| [`run-bench-pipeline.sh`](../scripts/run-bench-pipeline.sh) | повтор benchmark | validate → benchmark → report | — (ORC уже есть) | после smoke/pilot, без regenerate |
| [`run-bloom-ab.sh`](../scripts/run-bloom-ab.sh) | **Bloom A/B** | 2×generate → 2×validate → 2×benchmark → report | `0.1` | сравнение ORC **с bloom / без bloom** |

#### `run-bloom-ab.sh` — что это

Скрипт автоматизирует **A/B-тест ORC Bloom filters**: на одном `BASE` создаёт **два идентичных по seed/объёму датасета**, но:

| Вариант | HDFS path | `--orc-bloom-filter-columns` | Метка в отчёте |
|---|---|---|---|
| **nobloom** (контроль) | `$BASE/orc` | `none` | `dataset_label=nobloom` |
| **bloom** | `$BASE/orc_bloom` | `event_id,user_id,product_id,campaign_id` | `dataset_label=bloom` |

**7 последовательных YARN-job:**

1. generate nobloom → `$BASE/orc`
2. generate bloom → `$BASE/orc_bloom`
3. validate nobloom → `reports/raw/validation_nobloom/`
4. validate bloom → `reports/raw/validation_bloom/`
5. benchmark nobloom → `reports/raw/benchmark_nobloom/`
6. benchmark bloom → `reports/raw/benchmark_bloom/`
7. report (объединённый) → `reports/summary/bloom-ab-report.md`

Отчёт содержит **Benchmark Summary** (колонки `dataset`, `bloom_columns`) и **Bloom filter comparison** (nobloom vs bloom по `bytes_read` / p50).

Логи в текущем каталоге: `bloom-ab-generate-nobloom.log`, …, `bloom-ab-report.log`.

**Не путать с smoke:** smoke — один датасет и проверка пайплайна; bloom A/B — два датасета и сравнение оптимизации equality-фильтров.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export JAR=~/orc-bench/orc-bench-all.jar
TARGET_SIZE_TB=0.1 BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bloom-ab.sh
```

### Переменные окружения скриптов

Общие для всех `run-*.sh` и `submit-spark32.sh`:

| Переменная | Дефолт | Описание |
|---|---|---|
| `BASE` | smoke: `…/orc_test`; bloom A/B: `…/orc_test_pilot` | корень эксперимента на HDFS |
| `JAR` | `~/orc-bench/orc-bench-all.jar` | fat JAR приложения |
| `SEED` | `42` | seed generate / validate / фильтров benchmark |
| `TARGET_SIZE_TB` | см. скрипт | объём generate (`0.01` smoke, `0.1` bloom A/B) |
| `NUM_EXECUTORS` | `16` | workers (через `submit-spark32.sh`) |
| `EXECUTOR_MEMORY` | `8g` | память executor |
| `EXECUTOR_CORES` | `4` | ядра executor |
| `DRIVER_MEMORY` | `4g` | память driver |

Только benchmark-пайплайны:

| Переменная | Дефолт (`run-smoke`) | Дефолт (`run-bench-pipeline`) | Дефолт (`run-bloom-ab`) |
|---|---|---|---|
| `BENCHMARK_REPEAT_RUNS` | `3` | `3` | `5` |
| `BENCHMARK_WARMUP_RUNS` | `1` | `1` | `1` |
| `BENCHMARK_TIMESTAMP_WINDOW_DAYS` | `30` | `30` | `30` (в скрипте зашито) |
| `BENCHMARK_SCENARIOS` | `all` (не env в smoke) | `all` | `all` (зашито) |

Формат аргументов приложения: **`--key=value`** (обязательно `=`).

### Режимы приложения (`--mode`)

| Режим | Описание | Выход на HDFS |
|---|---|---|
| `generate` | синтетический датасет → ORC | `<orc-path>/` |
| `validate` | проверки качества данных | `<reports-validation-path>/` (parquet) |
| `benchmark` | 10 сценариев, метрики wall time + `bytes_read` | `<reports-benchmark-path>/` (parquet) |
| `report` | агрегация raw → summary | `<reports-path>/summary/` |

Дефолтные пути от `--base-path=$BASE`:

```text
$BASE/orc/                          # ORC-данные
$BASE/reports/raw/benchmark/        # benchmark metrics
$BASE/reports/raw/validation/       # validation results
$BASE/reports/summary/              # markdown + csv/json/parquet
```

Переопределение: `--orc-path`, `--reports-path`, `--reports-benchmark-path`, `--reports-validation-path`.

### Параметры: пути и общие

| Параметр | Обяз. | Дефолт | Режимы | Описание |
|---|---|---|---|---|
| `--mode` | да | — | все | `generate`, `validate`, `benchmark`, `report` |
| `--base-path` | нет | `hdfs:///user/hdfs_migration_user/orc_test` | все | корень эксперимента |
| `--orc-path` | нет | `<base>/orc` | generate, validate, benchmark | путь ORC |
| `--reports-path` | нет | `<base>/reports` | косвенно | корень отчётов |
| `--reports-benchmark-path` | нет | `<reports>/raw/benchmark` | benchmark, report | raw benchmark |
| `--reports-validation-path` | нет | `<reports>/raw/validation` | validate, report | raw validation |
| `--seed` | нет | `42` | generate, validate, benchmark | воспроизводимость |

### Параметры: generate

| Параметр | Дефолт | Описание |
|---|---|---|
| `--target-size-tb` | `5` | целевой объём (дроби OK: `0.01`, `0.1`) |
| `--avg-row-bytes` | `512` | средний размер строки |
| `--chunk-days` | `1` | окно чанка генерации |
| `--timestamp-start` | `2024-01-01` | начало диапазона timestamp |
| `--timestamp-end` | `2025-01-01` | конец (exclusive) |
| `--target-file-size-mb` | `384` | целевой размер файла |
| `--write-partitions` | авто | партиции при записи |
| `--partition-by` | `event_year,event_month,event_day,log_format` | partition columns |
| `--orc-compression` | `snappy` | `snappy`, `zstd`, `none` |
| `--orc-stripe-size-mb` | `64` | размер stripe |
| `--orc-row-group-size-mb` | `32` | row group |
| `--orc-bloom-filter-columns` | `event_id,user_id,product_id,campaign_id` | bloom при записи; `none` = выкл |
| `--orc-bloom-filter-fpp` | `0.05` | false positive rate bloom |

### Параметры: validate

| Параметр | Дефолт | Описание |
|---|---|---|
| `--validation-checks` | `all` | CSV или `all` |
| `--validation-sample-fraction` | `0.01` | доля выборки |
| `--log-format-share-tolerance` | `0.15` | допуск долей log_format |

Проверки (`all` включает все):

| Check | Описание |
|---|---|
| `row_count` | датасет непустой |
| `low_cardinality_bounds` | country/device/status/log_format в пределах |
| `timestamp_range` | timestamp в generate-окне |
| `log_format_distribution` | все форматы, баланс долей |
| `log_message_structure` | log_message не пустой |
| `orc_bloom_filters` | bloom index present/absent по `--orc-bloom-filter-columns` |

При FAIL любой проверки job падает.

### Параметры: benchmark

| Параметр | Дефолт | Описание |
|---|---|---|
| `--benchmark-warmup-runs` | `1` | прогрев (не пишется в raw) |
| `--benchmark-repeat-runs` | `3` | измеряемые повторы (p50/p95) |
| `--benchmark-scenarios` | `all` | CSV или `all` |
| `--benchmark-timestamp-window-days` | `30` | окно для `filter_timestamp_range` |
| `--clear-cache-between-runs` | `true` | cold read; иначе `bytes_read` занижен |
| `--benchmark-dataset-label` | auto | `bloom` / `nobloom` (метка в отчёте) |

Сценарии:

| Сценарий | Назначение | Bloom-релевантность |
|---|---|---|
| `full_scan` | baseline I/O | нет |
| `projection` | column pruning | нет |
| `filter_low_cardinality` | country + status | низкая |
| `filter_medium_cardinality` | product_id / campaign_id | **да** |
| `filter_high_cardinality` | event_id / user_id point lookup | **да (primary)** |
| `filter_timestamp_range` | range по timestamp | нет (range) |
| `filter_log_format` | equality log_format | partition pruning |
| `filter_combined` | timestamp + log_format + status | частично |
| `group_by` | агрегация | нет |
| `text_search` | substring log_message | нет |

Метрики в raw: `duration_ms`, `bytes_read`, `records_read`, `selectivity`, `dataset_label`, `orc_bloom_columns`.

### Параметры: report

| Параметр | Дефолт | Описание |
|---|---|---|
| `--report-formats` | `parquet,csv,json,markdown` | форматы summary |
| `--report-name` | `benchmark-report` | имя `.md` (бloom A/B: `bloom-ab-report`) |

Report читает **все** найденные каталоги: `benchmark_nobloom`, `benchmark_bloom`, `benchmark`, `validation_*`, `validation`.

### Какой скрипт когда

```text
Первый раз на кластере     →  run-smoke.sh          (0.01 TB)
Пайплайн OK, нужны выводы  →  Pilot §5 или run-bench-pipeline после generate ≥0.1 TB
Сравнить bloom vs nobloom  →  run-bloom-ab.sh       (≥0.1 TB, repeat≥5)
Один шаг / отладка         →  submit-spark32.sh -- --mode=...
```

---

## 3. Smoke (обязательно первым)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.

**Что даёт этот прогон:** проверка пайплайна end-to-end (generate → validate → benchmark → report), ресурсов YARN и новых метрик (`runs=3`, selective timestamp, `avg_bytes_read`).  
**Чего не даёт:** устойчивой картины по pruning/ранжированию сценариев — на `0.01` ТБ wall time часто «слипается» (фиксированный overhead job доминирует). Для интерпретации performance см. §5 Pilot.

Короткий путь:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
# при необходимости: export EXECUTOR_MEMORY=8g NUM_EXECUTORS=16
./scripts/run-smoke.sh
```

По шагам (эквивалент `run-smoke.sh`; ресурсы — дефолты `submit-spark32.sh`):

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export SEED=42

# 1. generate ORC (smoke-объём)
./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 --seed="$SEED" \
  2>&1 | tee smoke-generate.log

# 2. validate (тот же seed, что у generate)
./scripts/submit-spark32.sh -- \
  --mode=validate --base-path="$BASE" --seed="$SEED" \
  2>&1 | tee smoke-validate.log

# 3. benchmark ORC
#    - repeat-runs=3 → осмысленные p50/p95
#    - timestamp-window-days=30 → filter_timestamp_range не на всём generate-окне
#    - clear-cache-between-runs=true → cold read, видны bytes_read / pruning
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=3 \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  2>&1 | tee smoke-benchmark.log

# 4. report (читает raw/benchmark + raw/validation)
./scripts/submit-spark32.sh -- \
  --mode=report --base-path="$BASE" \
  2>&1 | tee smoke-report.log
```

### Критерий успеха smoke

- Все job в статусе `SUCCEEDED`
- Есть пути: `$BASE/orc`, `$BASE/reports/raw/benchmark/`, `$BASE/reports/raw/validation/`, `$BASE/reports/summary/`
- Markdown-отчёт содержит секции Benchmark Summary (с `avg_bytes_read`) и Validation (не пустая)
- У сценариев `runs=3`
- У `filter_timestamp_range` selectivity **существенно меньше 1.0** (ожидаемо ~окно/год, при 30d ≈ 0.08)
- У selective-фильтров `avg_bytes_read` **ниже**, чем у `full_scan` (иначе pushdown/pruning не проявляется — смотрите plan/логи)

Рекомендуемый порядок: **generate → validate → benchmark → report**.

Без повторного generate (уже есть ORC): `./scripts/run-bench-pipeline.sh`.

**Carbon A/B:** в этом репозитории не реализован (out of scope). Сравнение форматов — отдельный проект/ветка.

---

## 4. Проверки после прогона

```bash
hdfs dfs -du -h -s "$BASE"/orc "$BASE"/reports
hdfs dfs -ls -R "$BASE"/reports/summary | head
yarn application -list -appStates FINISHED | head

# applicationId из yarn / spark-submit:
yarn logs -applicationId application_XXXXXXXX_XXXX > yarn-app.log
```

---

## 5. Адекватная картина бенчмарка (Pilot)

Smoke (`--target-size-tb=0.01`) **недостаточен** для выводов по performance: на малом объёме wall time сценариев почти одинаковый, pruning/`bytes_read` плохо различимы.

Для **адекватной картины** после успешного smoke сделайте Pilot с большим датасетом и теми же корректными флагами benchmark.

### Рекомендуемые параметры

| Параметр | Значение | Зачем |
|---|---|---|
| `--target-size-tb` | **`0.1`** (минимум) или **`0.5`** | объём, на котором видны различия сценариев |
| `--seed` | `42` | воспроизводимость generate / validate / фильтров |
| `--benchmark-repeat-runs` | **`3`** (лучше **`5`**) | устойчивые p50/p95 |
| `--benchmark-warmup-runs` | `1` | прогрев JVM / метаданных |
| `--benchmark-timestamp-window-days` | `30` | selectivity ≪ 1 у `filter_timestamp_range` |
| `--clear-cache-between-runs` | `true` | cold read; иначе `bytes_read` недостоверен |
| `--benchmark-scenarios` | `all` | полный набор сценариев |
| `NUM_EXECUTORS` | `16` | workers |
| `EXECUTOR_MEMORY` | `8g` (при OOM — `16g`) | память worker |

**Не используйте** `--target-size-tb=0.01` как единственный прогон для отчёта о производительности.

### Полный прогон Pilot (по шагам)

Отдельный HDFS-корень удобен, чтобы не затирать smoke-данные:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export JAR=~/orc-bench/orc-bench-all.jar
export SEED=42
export NUM_EXECUTORS=16
export EXECUTOR_MEMORY=8g

TARGET_SIZE_TB=0.1          # для более уверенных выводов: 0.5
REPEAT_RUNS=5               # минимум 3; для адекватной картины лучше 5

hdfs dfs -mkdir -p "$BASE"

# 1. generate ORC (pilot-объём — обязательно ≥ 0.1)
./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" \
  --target-size-tb="$TARGET_SIZE_TB" --seed="$SEED" \
  2>&1 | tee pilot-generate.log

# 2. validate
./scripts/submit-spark32.sh -- \
  --mode=validate --base-path="$BASE" --seed="$SEED" \
  2>&1 | tee pilot-validate.log

# 3. benchmark
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs="$REPEAT_RUNS" \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  2>&1 | tee pilot-benchmark.log

# 4. report
./scripts/submit-spark32.sh -- \
  --mode=report --base-path="$BASE" \
  2>&1 | tee pilot-report.log
```

Короткий путь, если ORC уже сгенерирован с нужным `--target-size-tb`:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export SEED=42
BENCHMARK_REPEAT_RUNS=5 BENCHMARK_TIMESTAMP_WINDOW_DAYS=30 \
  ./scripts/run-bench-pipeline.sh
```

### Full (после Pilot)

| Этап | `--target-size-tb` |
|---|---|
| Full | по согласованию (дефолт приложения — `5`) |

Те же шаги 1–4 и те же benchmark-флаги; увеличьте `EXECUTOR_MEMORY` при OOM.

### Критерий: картина адекватна

Считайте прогон годным для выводов по бенчмарку, только если выполнено всё:

| Проверка | Ожидание |
|---|---|
| Объём generate | `--target-size-tb` ≥ **0.1** (не 0.01) |
| `runs` | ≥ 3, предпочтительно **5** |
| Validation | все checks **PASS**, секция не пустая |
| `filter_timestamp_range` selectivity | ≪ 1 (при 30d ≈ **0.08**, не ~1.0) |
| `avg_bytes_read` | у selective-фильтров **заметно ниже**, чем у `full_scan` |
| p50 / wall time | сценарии различаются сильнее, чем «все ~одинаковые ±несколько %» |
| Ресурсы | 16 executors, без постоянного OOM / недобора контейнеров |

Иначе сначала добейте smoke (§3), затем повторите Pilot с `TARGET_SIZE_TB=0.1` или `0.5`.

---

## 6. Bloom filter A/B (`run-bloom-ab.sh`)

Подробное описание скрипта и всех параметров — в **§2** (таблица скриптов, `run-bloom-ab.sh`, bloom-параметры).

Сравнение ORC **без bloom** (`$BASE/orc`) и **с bloom** (`$BASE/orc_bloom`) на колонках  
`event_id,user_id,product_id,campaign_id`.

Read-side включён для обоих прогонов: `spark.sql.orc.splits.include.file.footer=true`, cache stripe details.

### Быстрый запуск

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export JAR=~/orc-bench/orc-bench-all.jar
TARGET_SIZE_TB=0.1 BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bloom-ab.sh
```

Переопределение ресурсов YARN (до запуска скрипта):

```bash
export NUM_EXECUTORS=16 EXECUTOR_MEMORY=16g DRIVER_MEMORY=8g
TARGET_SIZE_TB=0.1 ./scripts/run-bloom-ab.sh
```

### Env-переменные `run-bloom-ab.sh`

| Переменная | Дефолт | Описание |
|---|---|---|
| `BASE` | `hdfs:///…/orc_test_pilot` | корень (отдельный от smoke!) |
| `SEED` | `42` | один seed для обоих датасетов |
| `TARGET_SIZE_TB` | `0.1` | объём **каждого** generate |
| `BENCHMARK_REPEAT_RUNS` | `5` | повторы benchmark |
| `BENCHMARK_WARMUP_RUNS` | `1` | прогрев |
| `NUM_EXECUTORS` / `EXECUTOR_MEMORY` / … | см. §1 | ресурсы YARN |

Bloom-колонки зашиты в скрипте: `event_id,user_id,product_id,campaign_id` (константа `BLOOM_COLUMNS`).

### Структура HDFS после прогона

```text
$BASE/orc/                              # nobloom (--orc-bloom-filter-columns=none)
$BASE/orc_bloom/                        # bloom (default columns)
$BASE/reports/raw/benchmark_nobloom/
$BASE/reports/raw/benchmark_bloom/
$BASE/reports/raw/validation_nobloom/
$BASE/reports/raw/validation_bloom/
$BASE/reports/summary/bloom-ab-report.md
```

### Критерии успеха Bloom A/B

| Проверка | Ожидание |
|---|---|
| Validation `orc_bloom_filters` | PASS на bloom path; PASS (absent) на nobloom |
| Report `Bloom filter comparison` | секция заполнена (есть `bloom` и `nobloom`) |
| `filter_high_cardinality` | `avg_bytes_read` bloom **ниже** nobloom |
| `full_scan` | bytes_read примерно одинаково (bloom не должен ломать scan) |

Primary сценарии: `filter_high_cardinality`, `filter_medium_cardinality`.

---

## 7. Сбор логов для анализа

```bash
APP_ID=application_XXXXXXXX_XXXX

mkdir -p ~/bench-logs && cd ~/bench-logs
cp ~/orc-bench/smoke-*.log . 2>/dev/null || true
cp ~/orc-bench/pilot-*.log . 2>/dev/null || true
cp ~/orc-bench/bloom-ab-*.log . 2>/dev/null || true
yarn logs -applicationId "$APP_ID" > yarn-${APP_ID}.log 2>&1

hdfs dfs -du -h -s "$BASE"/* > hdfs-du.txt 2>&1
hdfs dfs -ls -R "$BASE"/reports > hdfs-reports-ls.txt 2>&1
hdfs dfs -get "$BASE"/reports/summary ./summary 2>&1 || true

{
  java -version
  spark-submit --version
  hadoop version
} > env-versions.txt 2>&1

tar -czf bench-logs.tgz smoke-*.log yarn-*.log hdfs-*.txt env-versions.txt summary 2>/dev/null
```

### Минимум при падении

1. Полная команда `./scripts/submit-spark32.sh` и exit code
2. `yarn logs -applicationId …`
3. `java -version`, `spark-submit --version`
4. Текст exception / stack trace

### Минимум при успешном smoke

1. Файлы `smoke-*.log`
2. `hdfs-du.txt`
3. Каталог `summary/` (особенно `*.md`)

### Минимум при адекватном Pilot

1. Файлы `pilot-*.log`
2. Подтверждение `--target-size-tb` ≥ `0.1` в generate-логе
3. `summary/*.md` с `runs` ≥ 3 (лучше 5), Validation PASS, `avg_bytes_read`

---

## 8. Типичные ошибки submit

Ошибки ниже относятся к **инфраструктуре кластера**, не к коду `orc-bench`.  
`AppMain` стартует только после успешного submit.

### 8.1. YARN ResourceManager: `Connection refused` на `:8032`

**Симптом:** `ConfiguredRMFailoverProxyProvider` / `Call From … to …:8032 failed … Connection refused`.

**Что делать:**

```bash
yarn node -list
yarn rmadmin -getAllServiceState
grep -E 'yarn.resourcemanager\.(address|ha|hostname)' /etc/hadoop/conf/yarn-site.xml
```

### 8.2. SSL: `SSLContext does not support … algorithms: sdp-deployer`

**Смысл:** в Spark SSL-конфиге указано значение `sdp-deployer` вместо валидных TLS cipher suites.

**Что делать:** править платформенный SSL (Ambari / `spark.ssl.*`), не приложение.

### 8.3. Hive / HBase credentials зависают submit

`submit-spark32.sh` по умолчанию передаёт:

```bash
--conf spark.security.credentials.hive.enabled=false
--conf spark.security.credentials.hbase.enabled=false
```

Без этого Spark на submit пытается взять Hive/HBase tokens; при `Connection refused` на Metastore (`:9083`) или HBase RS (`:16020`) сабмит зависает на ретраях, и `AppMain` не стартует. Для ORC Metastore/HBase не нужны.

### 8.4. `Invalid numeric argument for --target-size-tb`

Нужен актуальный fat JAR с поддержкой дробных ТБ (`double`).

### 8.5. Чеклист перед повторным smoke

1. `yarn node -list` — есть RUNNING NodeManager’ы  
2. Нет ошибки `sdp-deployer` в SSL  
3. В логе после submit есть строки приложения (`orc-bench` / `Writing` / `Generating`)

При новом падении прислать полный `smoke-*.log` + `applicationId` + `yarn logs -applicationId …`.
