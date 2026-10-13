# Запуск на кластере HDFS dev1-abyss-sdp2-ambari-01

## Общая информация

**Кластер**: `dev1-abyss-sdp2-ambari-01.opsmon.sbt`  
**HDFS NameNode порт**: `50470` (по аналогии с README для ambari-02)  
**Требования**: Java 8, Spark 3.2.x, HDFS доступ

---

## Подготовка

### 1. Сборка проекта

```bash
gradlew.bat build
```

После сборки в `build/libs/` появится:
- `orc-carbon-bench-0.1.0-SNAPSHOT-all.jar` — fat JAR (используйте этот)

---

## Полный пайплайн

Рекомендуемый порядок: **generate → validate → benchmark → index-experiment → report**

### Пути на кластере (пример)

```bash
BASE=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon

# Дополнительные пути для index-experiment
BASELINE_PATH=$BASE/carbon-baseline
BLOOM_PATH=$BASE/carbon-bloom
LUCENE_PATH=$BASE/carbon-lucene
BLOOM_LUCENE_PATH=$BASE/carbon-bloom-lucene

REPORTS_PATH=$BASE/reports
```

---

## Шаг 1. Генерация данных (`--mode=generate`)

### Полная генерация 5 ТБ в оба формата (ORC + CarbonData)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --target-size-tb=5 \
  --seed=42 \
  --output-formats=orc,carbon \
  --orc-compression=snappy \
  --enable-bloom-index=true \
  --enable-lucene-index=true
```

### Только ORC

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=orc \
  --target-size-tb=1 \
  --orc-compression=zstd
```

### Только CarbonData с индексами

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --target-size-tb=1 \
  --enable-bloom-index=true \
  --bloom-index-columns=user_id,product_id,event_id \
  --enable-lucene-index=true
```

### Смог smoke-тест (0.01 ТБ)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --target-size-tb=0.01 \
  --seed=42
```

---

## Шаг 2. Валидация (`--mode=validate`)

**Должен пройти успешно перед benchmark!**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=validate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --validation-checks=all \
  --validation-sample-fraction=0.01
```

---

## Шаг 3. Бенчмарки (`--mode=benchmark`)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=benchmark \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --seed=42 \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=3 \
  --benchmark-scenarios=all
```

---

## Шаг 4. Индексные эксперименты (`--mode=index-experiment`)

**Требует отдельные CarbonData-пути для каждого профиля!**

### Вариант 1: Использование одного CarbonData-пути (базовый)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=index-experiment \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --carbon-baseline-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-baseline \
  --carbon-bloom-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom \
  --carbon-lucene-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-lucene \
  --carbon-bloom-lucene-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom-lucene \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --seed=42
```

### Вариант 2: Генерация отдельных таблиц для каждого профиля

```bash
# 1. baseline (без индексов)
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --carbon-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-baseline \
  --target-size-tb=1

# 2. bloom (только Bloom-индекс)
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --carbon-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom \
  --target-size-tb=1 \
  --enable-bloom-index=true

# 3. lucene (только Lucene-индекс)
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --carbon-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-lucene \
  --target-size-tb=1 \
  --enable-lucene-index=true

# 4. bloom_lucene (оба индекса)
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --carbon-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom-lucene \
  --target-size-tb=1 \
  --enable-bloom-index=true \
  --enable-lucene-index=true
```

После генерации запускайте `index-experiment` с соответствующими путями.

---

## Шаг 5. Формирование отчёта (`--mode=report`)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=report \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --report-formats=parquet,csv,json,markdown \
  --report-name=benchmark-report
```

Выходные файлы:
- `hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/reports/summary/results.parquet`
- `hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/reports/summary/results.csv`
- `hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/reports/summary/results.json`
- `hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon/reports/summary/benchmark-report.md`

---

## Сценарий полного прогона (copy-paste)

```bash
JAR=build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar
BASE=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon

# 1. Генерация в ORC + CarbonData
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=generate \
  --base-path="$BASE" \
  --target-size-tb=5 --seed=42 \
  --enable-bloom-index=true --enable-lucene-index=true

# 2. Валидация
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=validate \
  --base-path="$BASE"

# 3. Бенчмарки
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=benchmark \
  --base-path="$BASE"

# 4. Индексные эксперименты (с отдельными путями)
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=index-experiment \
  --base-path="$BASE" \
  --carbon-baseline-path="$BASE/carbon-baseline" \
  --carbon-bloom-path="$BASE/carbon-bloom" \
  --carbon-lucene-path="$BASE/carbon-lucene" \
  --carbon-bloom-lucene-path="$BASE/carbon-bloom-lucene" \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --seed=42

# 5. Отчёт
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=report \
  --base-path="$BASE"
```

---

## Полезные команды

### Проверка пути в HDFS

```bash
hdfs dfs -ls hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon
```

### Просмотр логов Spark-задачи

После запуска получите ID задачи (например, `application_XXXX_XXXX`) и выполните:

```bash
spark-submit --master yarn --deploy-mode cluster ... 2>&1 | grep "application_"
yarn logs -applicationId application_XXXX_XXXX
```

---

## Особенности кластера

- **Используйте только fat JAR** (`*-all.jar`)
- **Не передавайте `--packages`** — CarbonData уже в bundled
- **Spark версия должна быть 3.2.x** (совместимо с собранной версией)
- **Все пути — абсолютные HDFS** с hostname и портом (50470)
