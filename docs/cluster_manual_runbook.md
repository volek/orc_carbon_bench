# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Порядок: **smoke (`0.01`, с bloom) → Pilot / все проверки (`≥0.1`, Bloom A/B)** → при необходимости Full.  
Кластер **не меняем**: используется штатный SDP Spark 3.2.

**Bloom — обязательная часть всех проверок.** Дефолт приложения пишет bloom на  
`event_id,user_id,product_id,campaign_id`. Validate с `--validation-checks=all` всегда включает  
`orc_bloom_filters`. Generate и validate должны использовать **одинаковый**  
`--orc-bloom-filter-columns` (иначе check врёт или падает).

| Константа | Значение |
|---|---|
| Bloom ON | `event_id,user_id,product_id,campaign_id` |
| Bloom OFF | `none` |

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
| BASE (smoke) | `hdfs:///user/hdfs_migration_user/orc_test` |
| BASE (Pilot / A/B) | `hdfs:///user/hdfs_migration_user/orc_test_pilot` |

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export BLOOM_COLUMNS=event_id,user_id,product_id,campaign_id
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
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS"
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
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS"
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
generate (bloom ON) → validate (bloom ON) → benchmark → report
```

Для **всех проверок** на Pilot-объёме — Bloom A/B (nobloom + bloom):

```text
generate nobloom + generate bloom
  → validate nobloom + validate bloom
  → benchmark nobloom + benchmark bloom
  → report (сравнение)
```

Каждый шаг — отдельный `spark-submit` (отдельное YARN-приложение).  
Низкий уровень: всегда `scripts/submit-spark32.sh`.  
Высокий уровень: обёртки `run-*.sh` для типовых пайплайнов.

### Скрипты-обёртки

| Скрипт | Назначение | Шаги | Дефолт `TARGET_SIZE_TB` | Bloom | Когда использовать |
|---|---|---|---|---|---|
| [`submit-spark32.sh`](../scripts/submit-spark32.sh) | один `--mode=...` | 1 job | — | явно в args | ручной запуск, отладка |
| [`run-smoke.sh`](../scripts/run-smoke.sh) | smoke пайплайн | generate → validate → benchmark → report | `0.01` | **ON** (дефолт app) | **первый** прогон на кластере |
| [`run-bench-pipeline.sh`](../scripts/run-bench-pipeline.sh) | повтор без generate | validate → benchmark → report | — | как в уже записанном ORC | после smoke/pilot |
| [`run-bloom-ab.sh`](../scripts/run-bloom-ab.sh) | **все проверки + Bloom A/B** | 2×generate → 2×validate → 2×benchmark → report | `0.1` | nobloom **и** bloom | Pilot / адекватная картина |

#### `run-bloom-ab.sh` — полный набор проверок

Скрипт — канонический способ прогнать **все** проверки, включая bloom:

| Вариант | HDFS path | `--orc-bloom-filter-columns` | Метка в отчёте |
|---|---|---|---|
| **nobloom** (контроль) | `$BASE/orc` | `none` | `dataset_label=nobloom` |
| **bloom** | `$BASE/orc_bloom` | `event_id,user_id,product_id,campaign_id` | `dataset_label=bloom` |

**7 последовательных YARN-job:**

1. generate nobloom → `$BASE/orc`
2. generate bloom → `$BASE/orc_bloom`
3. validate nobloom → `reports/raw/validation_nobloom/` (`orc_bloom_filters`: absent)
4. validate bloom → `reports/raw/validation_bloom/` (`orc_bloom_filters`: present)
5. benchmark nobloom → `reports/raw/benchmark_nobloom/`
6. benchmark bloom → `reports/raw/benchmark_bloom/`
7. report → `reports/summary/bloom-ab-report.md`

Отчёт: **Benchmark Summary** (`dataset`, `bloom_columns`) + **Bloom filter comparison**  
(nobloom vs bloom по `bytes_read` / p50) + Validation по обоим вариантам.

Логи: `bloom-ab-generate-nobloom.log`, …, `bloom-ab-report.log`.

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export JAR=~/orc-bench/orc-bench-all.jar
TARGET_SIZE_TB=0.1 BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bloom-ab.sh
```

### Переменные окружения скриптов

| Переменная | Дефолт | Описание |
|---|---|---|
| `BASE` | smoke: `…/orc_test`; A/B: `…/orc_test_pilot` | корень эксперимента на HDFS |
| `JAR` | `~/orc-bench/orc-bench-all.jar` | fat JAR приложения |
| `SEED` | `42` | seed generate / validate / фильтров benchmark |
| `TARGET_SIZE_TB` | см. скрипт | объём generate (`0.01` smoke, `0.1` A/B) |
| `NUM_EXECUTORS` | `16` | workers |
| `EXECUTOR_MEMORY` | `8g` | память executor |
| `EXECUTOR_CORES` | `4` | ядра executor |
| `DRIVER_MEMORY` | `4g` | память driver |

Только benchmark-пайплайны:

| Переменная | Дефолт (`run-smoke`) | Дефолт (`run-bench-pipeline`) | Дефолт (`run-bloom-ab`) |
|---|---|---|---|
| `BENCHMARK_REPEAT_RUNS` | `3` | `3` | `5` |
| `BENCHMARK_WARMUP_RUNS` | `1` | `1` | `1` |
| `BENCHMARK_TIMESTAMP_WINDOW_DAYS` | `30` | `30` | `30` (в скрипте) |
| `BENCHMARK_SCENARIOS` | `all` | `all` | `all` |

Формат аргументов приложения: **`--key=value`** (обязательно `=`).

### Режимы приложения (`--mode`)

| Режим | Описание | Выход на HDFS |
|---|---|---|
| `generate` | синтетический датасет → ORC (+ bloom по флагам) | `<orc-path>/` |
| `validate` | проверки качества, **включая** `orc_bloom_filters` | `<reports-validation-path>/` |
| `benchmark` | 10 сценариев, wall time + `bytes_read` | `<reports-benchmark-path>/` |
| `report` | агрегация raw → summary | `<reports-path>/summary/` |

Дефолтные пути от `--base-path=$BASE` (одиночный прогон с bloom ON):

```text
$BASE/orc/                          # ORC с bloom (дефолт columns)
$BASE/reports/raw/benchmark/
$BASE/reports/raw/validation/
$BASE/reports/summary/
```

Bloom A/B пути — в §5.

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
| `--orc-bloom-filter-columns` | `event_id,user_id,product_id,campaign_id` | **bloom ON по умолчанию**; `none` = выкл |
| `--orc-bloom-filter-fpp` | `0.05` | false positive rate bloom |

### Параметры: validate

| Параметр | Дефолт | Описание |
|---|---|---|
| `--validation-checks` | `all` | CSV или `all` (**включает** `orc_bloom_filters`) |
| `--validation-sample-fraction` | `0.01` | доля выборки |
| `--log-format-share-tolerance` | `0.15` | допуск долей log_format |
| `--orc-bloom-filter-columns` | те же, что у generate | ожидание present/absent для bloom check |

Проверки (`all` = полный набор; bloom обязателен):

| Check | Описание |
|---|---|
| `row_count` | датасет непустой |
| `low_cardinality_bounds` | country/device/status/log_format в пределах |
| `timestamp_range` | timestamp в generate-окне |
| `log_format_distribution` | все форматы, баланс долей |
| `log_message_structure` | log_message не пустой |
| `orc_bloom_filters` | bloom index **present**, если columns ≠ `none`; **absent**, если `none` |

При FAIL любой проверки job падает; в diagnostics перечисляются упавшие checks и `details`.

**Правило:** флаги bloom у generate и validate должны совпадать.

### Параметры: benchmark

| Параметр | Дефолт | Описание |
|---|---|---|
| `--benchmark-warmup-runs` | `1` | прогрев (не пишется в raw) |
| `--benchmark-repeat-runs` | `3` | измеряемые повторы (p50/p95) |
| `--benchmark-scenarios` | `all` | CSV или `all` |
| `--benchmark-timestamp-window-days` | `30` | окно для `filter_timestamp_range` |
| `--clear-cache-between-runs` | `true` | cold read; иначе `bytes_read` занижен |
| `--benchmark-dataset-label` | auto | `bloom` / `nobloom` |

Сценарии:

| Сценарий | Назначение | Bloom-релевантность |
|---|---|---|
| `full_scan` | baseline I/O | нет (контроль) |
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
| `--report-name` | `benchmark-report` | имя `.md` (A/B: `bloom-ab-report`) |

Report читает **все** найденные каталоги: `benchmark_nobloom`, `benchmark_bloom`, `benchmark`, `validation_*`, `validation`.

### Какой скрипт когда

```text
Первый раз на кластере              →  run-smoke.sh           (0.01 TB, bloom ON)
Все проверки / адекватная картина   →  run-bloom-ab.sh        (≥0.1 TB, nobloom+bloom)
Повтор без regenerate               →  run-bench-pipeline.sh  (ORC уже с bloom)
Один шаг / отладка                  →  submit-spark32.sh -- --mode=...
```

---

## 3. Smoke (обязательно первым, с bloom)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.  
Датасет пишется **с bloom** (дефолт app / явный `--orc-bloom-filter-columns`).

**Что даёт:** end-to-end пайплайн, YARN, validation включая `orc_bloom_filters` (present), метрики (`runs=3`, `avg_bytes_read`).  
**Чего не даёт:** устойчивого A/B bloom vs nobloom и ранжирования на малом объёме — для этого §5.

Короткий путь:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
# при необходимости: export EXECUTOR_MEMORY=8g NUM_EXECUTORS=16
./scripts/run-smoke.sh
```

`run-smoke.sh` не передаёт bloom-флаг явно — срабатывает **дефолт приложения** (bloom ON).  
При ручном запуске указывайте колонки явно, чтобы generate и validate совпали.

По шагам:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export SEED=42
export BLOOM_COLUMNS=event_id,user_id,product_id,campaign_id

# 1. generate ORC с bloom
./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee smoke-generate.log

# 2. validate (тот же seed и те же bloom-колонки)
./scripts/submit-spark32.sh -- \
  --mode=validate --base-path="$BASE" --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee smoke-validate.log

# 3. benchmark
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=3 \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  --benchmark-dataset-label=bloom \
  2>&1 | tee smoke-benchmark.log

# 4. report
./scripts/submit-spark32.sh -- \
  --mode=report --base-path="$BASE" \
  2>&1 | tee smoke-report.log
```

### Критерий успеха smoke

- Все job `SUCCEEDED`
- Пути: `$BASE/orc`, `$BASE/reports/raw/benchmark/`, `$BASE/reports/raw/validation/`, `$BASE/reports/summary/`
- Validation: **все** checks PASS, в т.ч. `orc_bloom_filters` (bloom present)
- Markdown: Benchmark Summary с `avg_bytes_read`, Validation не пустая, `runs=3`
- У `filter_timestamp_range` selectivity ≪ 1 (при 30d ≈ 0.08)
- У selective-фильтров `avg_bytes_read` ниже, чем у `full_scan`

Без повторного generate: `./scripts/run-bench-pipeline.sh` (ORC уже с bloom).

**Carbon A/B:** в этом репозитории не реализован (out of scope).

---

## 4. Проверки после прогона

```bash
hdfs dfs -du -h -s "$BASE"/orc "$BASE"/orc_bloom "$BASE"/reports 2>/dev/null
hdfs dfs -ls -R "$BASE"/reports/summary | head
yarn application -list -appStates FINISHED | head

yarn logs -applicationId application_XXXXXXXX_XXXX > yarn-app.log
```

---

## 5. Все проверки / адекватная картина (Pilot + Bloom A/B)

Smoke недостаточен для выводов по performance и **не сравнивает** bloom vs nobloom.

Канонический Pilot = **`run-bloom-ab.sh`**: оба датасета, оба validate (present/absent), оба benchmark, единый отчёт.

### Рекомендуемые параметры

| Параметр | Значение | Зачем |
|---|---|---|
| `--target-size-tb` | **`0.1`** (минимум) или **`0.5`** | различимы сценарии и bloom-эффект |
| `--seed` | `42` | одинаковый seed для nobloom и bloom |
| `--benchmark-repeat-runs` | **`5`** (минимум 3) | устойчивые p50/p95 |
| `--benchmark-warmup-runs` | `1` | прогрев |
| `--benchmark-timestamp-window-days` | `30` | selectivity ≪ 1 |
| `--clear-cache-between-runs` | `true` | cold read / достоверный `bytes_read` |
| Bloom columns | `event_id,user_id,product_id,campaign_id` | запись + validate present |
| Контроль | `--orc-bloom-filter-columns=none` | validate absent |
| `NUM_EXECUTORS` | `16` | workers |
| `EXECUTOR_MEMORY` | `8g` (при OOM — `16g`) | память worker |

**Не используйте** `--target-size-tb=0.01` как единственный прогон для отчёта о производительности / bloom.

### Быстрый запуск (рекомендуется)

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export JAR=~/orc-bench/orc-bench-all.jar
export NUM_EXECUTORS=16
export EXECUTOR_MEMORY=8g

hdfs dfs -mkdir -p "$BASE"
TARGET_SIZE_TB=0.1 BENCHMARK_REPEAT_RUNS=5 ./scripts/run-bloom-ab.sh
```

### Структура HDFS после полного прогона

```text
$BASE/orc/                              # nobloom
$BASE/orc_bloom/                        # bloom
$BASE/reports/raw/benchmark_nobloom/
$BASE/reports/raw/benchmark_bloom/
$BASE/reports/raw/validation_nobloom/
$BASE/reports/raw/validation_bloom/
$BASE/reports/summary/bloom-ab-report.md
```

### Env `run-bloom-ab.sh`

| Переменная | Дефолт | Описание |
|---|---|---|
| `BASE` | `hdfs:///…/orc_test_pilot` | отдельный от smoke корень |
| `SEED` | `42` | один seed на оба датасета |
| `TARGET_SIZE_TB` | `0.1` | объём **каждого** generate |
| `BENCHMARK_REPEAT_RUNS` | `5` | повторы benchmark |
| `BENCHMARK_WARMUP_RUNS` | `1` | прогрев |
| `NUM_EXECUTORS` / `EXECUTOR_MEMORY` / … | см. §1 | ресурсы YARN |

Bloom-колонки зашиты в скрипте (`BLOOM_COLUMNS`).

### Критерий: все проверки пройдены / картина адекватна

| Проверка | Ожидание |
|---|---|
| Объём | `--target-size-tb` ≥ **0.1** |
| `runs` | ≥ 3, предпочтительно **5** |
| Validation nobloom | все PASS; `orc_bloom_filters` = absent |
| Validation bloom | все PASS; `orc_bloom_filters` = present |
| Report | секции Benchmark Summary + **Bloom filter comparison** + Validation |
| `filter_high_cardinality` | `avg_bytes_read` bloom **ниже** nobloom |
| `filter_medium_cardinality` | желательно выигрыш bloom по bytes_read |
| `full_scan` | bytes_read примерно одинаково |
| `filter_timestamp_range` selectivity | ≪ 1 (30d ≈ **0.08**) |
| Ресурсы | без постоянного OOM / недобора контейнеров |

### Одиночный Pilot только с bloom (без A/B)

Если нужен один датасет с bloom (без сравнения nobloom) — тот же объём и флаги, но **не** заменяет полный A/B:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test_pilot
export SEED=42
export BLOOM_COLUMNS=event_id,user_id,product_id,campaign_id
TARGET_SIZE_TB=0.1
REPEAT_RUNS=5

./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" \
  --target-size-tb="$TARGET_SIZE_TB" --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee pilot-generate.log

./scripts/submit-spark32.sh -- \
  --mode=validate --base-path="$BASE" --seed="$SEED" \
  --orc-bloom-filter-columns="$BLOOM_COLUMNS" \
  2>&1 | tee pilot-validate.log

./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --seed="$SEED" \
  --benchmark-scenarios=all \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs="$REPEAT_RUNS" \
  --benchmark-timestamp-window-days=30 \
  --clear-cache-between-runs=true \
  --benchmark-dataset-label=bloom \
  2>&1 | tee pilot-benchmark.log

./scripts/submit-spark32.sh -- \
  --mode=report --base-path="$BASE" \
  2>&1 | tee pilot-report.log
```

### Full (после Pilot)

| Этап | `--target-size-tb` |
|---|---|
| Full | по согласованию (дефолт приложения — `5`) |

Тот же Bloom A/B (`run-bloom-ab.sh` с большим `TARGET_SIZE_TB`); при OOM увеличьте `EXECUTOR_MEMORY`.

---

## 6. Сбор логов для анализа

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

tar -czf bench-logs.tgz smoke-*.log bloom-ab-*.log yarn-*.log hdfs-*.txt env-versions.txt summary 2>/dev/null
```

### Минимум при падении

1. Полная команда `./scripts/submit-spark32.sh` / `run-*.sh` и exit code
2. `yarn logs -applicationId …` (в diagnostics — имена упавших validation checks)
3. `java -version`, `spark-submit --version`
4. Совпадение `--orc-bloom-filter-columns` у generate и validate

### Минимум при успешном smoke

1. `smoke-*.log`
2. `hdfs-du.txt`
3. `summary/` с Validation PASS (включая bloom present) и `avg_bytes_read`

### Минимум при полном Pilot (Bloom A/B)

1. `bloom-ab-*.log`
2. `--target-size-tb` ≥ `0.1` в generate-логах
3. `bloom-ab-report.md`: Validation PASS на обоих путях, секция Bloom comparison, `runs` ≥ 5

---

## 7. Типичные ошибки submit

Ошибки ниже относятся к **инфраструктуре кластера**, не к коду `orc-bench`.  
`AppMain` стартует только после успешного submit.

### 7.1. YARN ResourceManager: `Connection refused` на `:8032`

**Симптом:** `ConfiguredRMFailoverProxyProvider` / `Call From … to …:8032 failed … Connection refused`.

**Что делать:**

```bash
yarn node -list
yarn rmadmin -getAllServiceState
grep -E 'yarn.resourcemanager\.(address|ha|hostname)' /etc/hadoop/conf/yarn-site.xml
```

### 7.2. SSL: `SSLContext does not support … algorithms: sdp-deployer`

**Смысл:** в Spark SSL-конфиге указано значение `sdp-deployer` вместо валидных TLS cipher suites.

**Что делать:** править платформенный SSL (Ambari / `spark.ssl.*`), не приложение.

### 7.3. Hive / HBase credentials зависают submit

`submit-spark32.sh` по умолчанию передаёт:

```bash
--conf spark.security.credentials.hive.enabled=false
--conf spark.security.credentials.hbase.enabled=false
```

Без этого Spark на submit пытается взять Hive/HBase tokens; при `Connection refused` на Metastore (`:9083`) или HBase RS (`:16020`) сабмит зависает на ретраях, и `AppMain` не стартует. Для ORC Metastore/HBase не нужны.

### 7.4. `Invalid numeric argument for --target-size-tb`

Нужен актуальный fat JAR с поддержкой дробных ТБ (`double`).

### 7.5. Validation: `orc_bloom_filters` / «No ORC data files»

- Generate и validate с **разным** `--orc-bloom-filter-columns` → present/absent не совпадает.
- Нужен актуальный JAR (инспектор обходит Hive partition dirs и читает bloom index через `sargColumns`).
- В diagnostics смотрите имя check и `details`.

### 7.6. Чеклист перед повторным smoke

1. `yarn node -list` — есть RUNNING NodeManager’ы  
2. Нет ошибки `sdp-deployer` в SSL  
3. В логе после submit есть строки приложения (`orc-bench` / `Writing` / `Generating`)
4. Bloom: generate пишет с columns (не `none`), validate — с теми же columns

При новом падении прислать полный `smoke-*.log` / `bloom-ab-*.log` + `applicationId` + `yarn logs -applicationId …`.
