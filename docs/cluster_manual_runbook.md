# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Порядок: **smoke (`0.01`) → Pilot / адекватная картина (`≥0.1`)** → при необходимости Full.  
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

Приоритет: **явный флаг до `--`** > **env** > **дефолт скрипта**.  
`run-smoke.sh` / `run-bench-pipeline.sh` вызывают `submit-spark32.sh` без своих spark-флагов — наследуют эти дефолты (или ваш `export`).

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

## 2. Smoke (обязательно первым)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.

**Что даёт этот прогон:** проверка пайплайна end-to-end (generate → validate → benchmark → report), ресурсов YARN и новых метрик (`runs=3`, selective timestamp, `avg_bytes_read`).  
**Чего не даёт:** устойчивой картины по pruning/ранжированию сценариев — на `0.01` ТБ wall time часто «слипается» (фиксированный overhead job доминирует). Для интерпретации performance см. §4 Pilot.

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

## 3. Проверки после прогона

```bash
hdfs dfs -du -h -s "$BASE"/orc "$BASE"/reports
hdfs dfs -ls -R "$BASE"/reports/summary | head
yarn application -list -appStates FINISHED | head

# applicationId из yarn / spark-submit:
yarn logs -applicationId application_XXXXXXXX_XXXX > yarn-app.log
```

---

## 4. Адекватная картина бенчмарка (Pilot)

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

Иначе сначала добейте smoke (§2), затем повторите Pilot с `TARGET_SIZE_TB=0.1` или `0.5`.

---

## 5. Сбор логов для анализа

```bash
APP_ID=application_XXXXXXXX_XXXX

mkdir -p ~/bench-logs && cd ~/bench-logs
cp ~/orc-bench/smoke-*.log . 2>/dev/null || true
cp ~/orc-bench/pilot-*.log . 2>/dev/null || true
cp ~/orc-bench/bench-*.log . 2>/dev/null || true
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

## 6. Типичные ошибки submit

Ошибки ниже относятся к **инфраструктуре кластера**, не к коду `orc-bench`.  
`AppMain` стартует только после успешного submit.

### 6.1. YARN ResourceManager: `Connection refused` на `:8032`

**Симптом:** `ConfiguredRMFailoverProxyProvider` / `Call From … to …:8032 failed … Connection refused`.

**Что делать:**

```bash
yarn node -list
yarn rmadmin -getAllServiceState
grep -E 'yarn.resourcemanager\.(address|ha|hostname)' /etc/hadoop/conf/yarn-site.xml
```

### 6.2. SSL: `SSLContext does not support … algorithms: sdp-deployer`

**Смысл:** в Spark SSL-конфиге указано значение `sdp-deployer` вместо валидных TLS cipher suites.

**Что делать:** править платформенный SSL (Ambari / `spark.ssl.*`), не приложение.

### 6.3. Hive / HBase credentials зависают submit

`submit-spark32.sh` по умолчанию передаёт:

```bash
--conf spark.security.credentials.hive.enabled=false
--conf spark.security.credentials.hbase.enabled=false
```

Без этого Spark на submit пытается взять Hive/HBase tokens; при `Connection refused` на Metastore (`:9083`) или HBase RS (`:16020`) сабмит зависает на ретраях, и `AppMain` не стартует. Для ORC Metastore/HBase не нужны.

### 6.4. `Invalid numeric argument for --target-size-tb: 0.01`

Нужен актуальный fat JAR с поддержкой дробных ТБ (`double`).

### 6.5. Чеклист перед повторным smoke

1. `yarn node -list` — есть RUNNING NodeManager’ы  
2. Нет ошибки `sdp-deployer` в SSL  
3. В логе после submit есть строки приложения (`orc-bench` / `Writing` / `Generating`)

При новом падении прислать полный `smoke-*.log` + `applicationId` + `yarn logs -applicationId …`.
