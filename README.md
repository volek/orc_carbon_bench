# orc-carbon-bench

Spark 3 / Java 8 приложение для сравнения форматов хранения ORC и CarbonData на HDFS.

## Требования

- Java 8 (совместимо с JVM кластера)
- Spark **3.2.x** кластер
- HDFS (или совместимое хранилище)
- Fat JAR со встроенным CarbonData (`*-all.jar`) — отдельный `--packages` не нужен

## Сборка

```bash
./gradlew build
```

Windows:

```bash
gradlew.bat build
```

Артефакты:
- `build/libs/orc-carbon-bench-0.1.0-SNAPSHOT.jar` — тонкий JAR
- `build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar` — **fat JAR** (приложение + CarbonData 2.3.0 / Spark 3.1 module, без Spark/Hadoop)

Версии сборки: Spark compile `3.2.1`, CarbonData `org.apache.carbondata:carbondata-spark_3.1:2.3.0`.

## Формат аргументов

Все параметры приложения передаются в формате `--ключ=значение`.

- Каждый аргумент должен начинаться с `--` и содержать `=`.
- Ключ и значение не могут быть пустыми.
- Регистр значения `--mode` не важен (`generate` и `GENERATE` эквивалентны).

## Обзор пайплайна

```text
generate  →  validate  →  benchmark  →  index-experiment  →  report
```

| Шаг | `--mode` | Выход | Статус |
|---|---|---|---|
| 1. Генерация данных | `generate` | `<orc-path>/`, `<carbon-path>/` | реализован |
| 2. Бенчмарки | `benchmark` | `<reports-path>/raw/` | реализован |
| 3. Индексные эксперименты | `index-experiment` | `<reports-path>/raw/index/` | реализован |
| 4. Валидация | `validate` | `<reports-path>/raw/validation/` | реализован |
| 5. Отчёт | `report` | `<reports-path>/summary/` | реализован |

Промежуточный слой Parquet **не используется** — данные генерируются и сразу записываются в ORC и CarbonData.

### Конфигурируемые пути HDFS

Все пути задаются независимо. Если явный путь не указан, он вычисляется от `--base-path`:

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--base-path` | `/bench/orc-carbon` | Корневой путь эксперимента |
| `--orc-path` | `<base-path>/orc` | Путь для ORC-данных |
| `--carbon-path` | `<base-path>/carbon` | Путь для CarbonData |
| `--reports-path` | `<base-path>/reports` | Путь для отчётов бенчмарков |

Пример для целевого кластера:

```bash
--base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
--orc-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/orc \
--carbon-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/carbon \
--reports-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/reports
```

Структура каталогов:

```text
<orc-path>/          # ORC-файлы
<carbon-path>/       # CarbonData-таблица
<reports-path>/
  raw/               # сырые метрики бенчмарков
  summary/           # агрегированные отчёты
```

### Общие параметры (все шаги)

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | Режим: `generate`, `validate`, `benchmark`, `index-experiment`, `report` |
| `--base-path` | нет | `/bench/orc-carbon` | Корневой путь (используется для вычисления путей по умолчанию) |
| `--orc-path` | нет | `<base-path>/orc` | Абсолютный HDFS-путь для ORC |
| `--carbon-path` | нет | `<base-path>/carbon` | Абсолютный HDFS-путь для CarbonData |
| `--reports-path` | нет | `<base-path>/reports` | Абсолютный HDFS-путь для отчётов |

---

## Шаг 1. Генерация данных (`--mode=generate`)

Генерирует синтетический датасет и **сразу записывает** его в ORC и/или CarbonData. Промежуточный Parquet не создаётся.

По умолчанию пишет в оба формата (`--output-formats=orc,carbon`).

### Параметры запуска

#### Общие

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `generate` |
| `--base-path` | нет | `/bench/orc-carbon` | Корневой путь |
| `--orc-path` | нет | `<base-path>/orc` | HDFS-путь для ORC |
| `--carbon-path` | нет | `<base-path>/carbon` | HDFS-путь для CarbonData |
| `--output-formats` | нет | `orc,carbon` | Форматы записи: `orc`, `carbon` или оба через запятую |
| `--target-size-tb` | нет | `5` | Целевой объём датасета в терабайтах (допускаются дроби, например `0.01`) |
| `--seed` | нет | `42` | Seed генератора для воспроизводимости |
| `--avg-row-bytes` | нет | `512` | Средний размер строки в байтах |
| `--chunk-days` | нет | `1` | Размер временного окна одного чанка в днях |
| `--timestamp-start` | нет | `2024-01-01` | Начало диапазона `timestamp` (`YYYY-MM-DD` или epoch ms) |
| `--timestamp-end` | нет | `2025-01-01` | Конец диапазона `timestamp` (не включительно) |
| `--target-file-size-mb` | нет | `384` | Целевой размер выходного файла в MB |
| `--write-partitions` | нет | авто | Число партиций при записи |
| `--partition-by` | нет | `event_year,event_month,event_day,log_format` | Колонки партиционирования |

#### ORC

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--orc-compression` | нет | `snappy` | Сжатие: `snappy`, `zstd`, `none` |
| `--orc-stripe-size-mb` | нет | `64` | Размер ORC stripe в MB |
| `--orc-row-group-size-mb` | нет | `32` | Размер row group в MB |

#### CarbonData

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--carbon-table-name` | нет | `bench_events` | Имя CarbonData таблицы |
| `--carbon-compression` | нет | `snappy` | Сжатие: `snappy`, `zstd`, `none` |
| `--enable-bloom-index` | нет | `false` | Создать Bloom-индекс после записи |
| `--bloom-index-columns` | нет | `user_id,product_id` | Колонки Bloom-индекса |
| `--enable-lucene-index` | нет | `false` | Создать Lucene-индекс после записи |
| `--lucene-index-columns` | нет | `log_message` | Колонки Lucene-индекса |

### Конфигурация Spark

Приложение автоматически выставляет:

```properties
# ORC
spark.sql.orc.filterPushdown=true
spark.sql.orc.enableVectorizedReader=true
spark.sql.orc.block.size=<orc-stripe-size-mb * 1024^2>

# CarbonData
spark.sql.extensions=org.apache.spark.sql.CarbonExtensions
spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog
```

CarbonData уже внутри fat JAR — **не** передавайте `--packages`.

### Расчёты

```text
estimated_rows     = target_size_tb * 1024^4 / avg_row_bytes
chunk_count        = ceil((timestamp_end - timestamp_start) / chunk_days)
rows_per_chunk     = ceil(estimated_rows / chunk_count)
write_partitions   = ceil(chunk_rows * avg_row_bytes / target_file_size_mb / 1024^2)
```

Генерация выполняется чанками. Каждый чанк записывается одновременно в выбранные форматы (`overwrite` для первого чанка, `append` для последующих). Bloom/Lucene индексы создаются один раз после завершения всех чанков.

### Схема данных

| Колонка | Кардинальность | Описание |
|---|---|---|
| `event_id` | высокая | Уникальный ID события |
| `user_id` | высокая | ID пользователя (~50M уникальных) |
| `session_id` | высокая | ID сессии (~100M уникальных) |
| `country_code` | низкая | Код страны (50 значений) |
| `device_type` | низкая | mobile, desktop, tablet, tv, iot |
| `status` | низкая | success, failed, pending, timeout |
| `product_id` | средняя | ID продукта (~100K, Zipf-скос) |
| `campaign_id` | средняя | ID кампании (~50K, Zipf-скос) |
| `region_id` | средняя | ID региона (~10K, Zipf-скос) |
| `timestamp` | — | Временная метка события |
| `amount` | — | Сумма транзакции (0–10000) |
| `payload_json` | — | JSON-представление события |
| `log_format` | низкая | json, plain_text, key_value, apache_common |
| `log_message` | — | Строка лога (для Lucene-тестов) |

Партиционирование: `event_year`, `event_month`, `event_day`, `log_format`.

### Примеры запуска

Полная генерация 5 ТБ в ORC и CarbonData:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --target-size-tb=5 \
  --seed=42 \
  --output-formats=orc,carbon \
  --orc-compression=snappy \
  --enable-bloom-index=true \
  --enable-lucene-index=true
```

Только ORC:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=orc \
  --target-size-tb=1 \
  --orc-compression=zstd
```

Только CarbonData с индексами:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --output-formats=carbon \
  --target-size-tb=1 \
  --enable-bloom-index=true \
  --bloom-index-columns=user_id,product_id,event_id \
  --enable-lucene-index=true
```

---

## Шаг 2. Бенчмарки (`--mode=benchmark`)

Запускает тесты производительности на данных из `<orc-path>/` и `<carbon-path>/`, сохраняет метрики в `<reports-path>/raw/`.

### Сценарии

| Сценарий | Описание |
|---|---|---|
| `full_scan` | Полное сканирование |
| `projection` | Выбор подмножества колонок |
| `filter_low_cardinality` | Фильтр по `country_code`, `status` |
| `filter_medium_cardinality` | Фильтр по `product_id`, `campaign_id` |
| `filter_high_cardinality` | Point lookup по `event_id`, `user_id` |
| `filter_timestamp_range` | Range-фильтр по `timestamp` |
| `filter_log_format` | Фильтр по `log_format` |
| `filter_combined` | Комбинированный фильтр |
| `group_by` | Агрегация `GROUP BY` |
| `lucene_text_search` | Поиск по `log_message` (только CarbonData) |

### Метрики (Parquet в `<reports-path>/raw/`)

| Поле | Описание |
|---|---|---|
| `run_id` | ID прогона |
| `scenario` | Имя сценария |
| `format` | `orc` или `carbon` |
| `run_index` | Номер измеряемого повтора |
| `warmup` | Прогревочный запуск (всегда `false` в файле) |
| `duration_ms` | Время выполнения |
| `rows_returned` | Число строк результата |
| `total_rows` | Общее число строк в датасете |
| `selectivity` | `rows_returned / total_rows` |
| `seed` | Seed эксперимента |
| `executed_at` | Время выполнения (ISO-8601) |

Прогрев (`--benchmark-warmup-runs`) выполняется без записи в отчёт. При `--clear-cache-between-runs=true` кэш Spark очищается перед каждым измеряемым прогоном.

### Параметры запуска

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `benchmark` |
| `--base-path` | нет | `/bench/orc-carbon` | Корневой путь |
| `--orc-path` | нет | `<base-path>/orc` | HDFS-путь к ORC-данным |
| `--carbon-path` | нет | `<base-path>/carbon` | HDFS-путь к CarbonData |
| `--reports-path` | нет | `<base-path>/reports` | HDFS-путь для отчётов |
| `--seed` | нет | `42` | Seed для воспроизводимости фильтров |
| `--benchmark-warmup-runs` | нет | `1` | Прогревочные запуски |
| `--benchmark-repeat-runs` | нет | `3` | Измеряемые повторы |
| `--benchmark-scenarios` | нет | `all` | Сценарии через запятую или `all` |
| `--clear-cache-between-runs` | нет | `true` | Очистка кэша между прогонами |
| `--formats` | нет | `orc,carbon` | Форматы для сравнения |

### Пример запуска

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=benchmark \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --seed=42
```

---

## Шаг 3. Индексные эксперименты (`--mode=index-experiment`)

Сравнивает ORC (reference) с CarbonData в профилях `baseline`, `bloom`, `lucene`, `bloom_lucene`. Запускает индексные сценарии и Lucene-поиск в разрезе каждого `log_format`.

Результаты:
- `<reports-path>/raw/index/` — метрики запросов
- `<reports-path>/raw/index/build-metrics/` — время построения индексов (при `--rebuild-indexes=true`)

### Профили индексов

| Профиль | Bloom | Lucene | Назначение |
|---|---|---|---|
| `baseline` | нет | нет | Базовое сравнение без вторичных индексов |
| `bloom` | да | нет | Point lookup по high/medium cardinality |
| `lucene` | нет | да | Текстовый поиск по `log_message` |
| `bloom_lucene` | да | да | Комбинированный сценарий |

### Сценарии

| Сценарий | ORC | CarbonData |
|---|---|---|
| `filter_high_cardinality` | да | да |
| `filter_medium_cardinality` | да | да |
| `lucene_text_search` | нет | да (для профилей с Lucene) |
| `lucene_text_search` × `log_format` | нет | да (json, plain_text, key_value, apache_common) |

### Параметры запуска

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `index-experiment` |
| `--orc-path` | нет | `<base-path>/orc` | ORC reference dataset |
| `--carbon-path` | нет | `<base-path>/carbon` | CarbonData по умолчанию |
| `--index-profiles` | нет | `baseline,bloom,lucene,bloom_lucene` | Профили для сравнения |
| `--carbon-baseline-path` | нет | `<carbon-path>` | Путь CarbonData для профиля baseline |
| `--carbon-bloom-path` | нет | `<carbon-path>` | Путь для профиля bloom |
| `--carbon-lucene-path` | нет | `<carbon-path>` | Путь для профиля lucene |
| `--carbon-bloom-lucene-path` | нет | `<carbon-path>` | Путь для профиля bloom_lucene |
| `--rebuild-indexes` | нет | `false` | Пересоздать индексы и замерить `build_time_ms` |
| `--benchmark-warmup-runs` | нет | `1` | Прогревочные запуски |
| `--benchmark-repeat-runs` | нет | `3` | Измеряемые повторы |
| `--clear-cache-between-runs` | нет | `true` | Очистка кэша между прогонами |
| `--bloom-index-columns` | нет | `user_id,product_id` | Колонки Bloom-индекса |
| `--lucene-index-columns` | нет | `log_message` | Колонки Lucene-индекса |

Для корректного сравнения `baseline` vs `bloom` vs `lucene` рекомендуется сгенерировать отдельные CarbonData-наборы в разные пути:

```bash
# baseline
--mode=generate --output-formats=carbon --carbon-path=.../carbon-baseline

# bloom
--mode=generate --output-formats=carbon --carbon-path=.../carbon-bloom \
  --enable-bloom-index=true

# lucene
--mode=generate --output-formats=carbon --carbon-path=.../carbon-lucene \
  --enable-lucene-index=true
```

### Пример запуска

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=index-experiment \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --carbon-baseline-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/carbon-baseline \
  --carbon-bloom-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom \
  --carbon-lucene-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/carbon-lucene \
  --carbon-bloom-lucene-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon/carbon-bloom-lucene \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --seed=42
```

---

## Шаг 4. Валидация данных (`--mode=validate`)

Проверяет корректность и согласованность данных в ORC и CarbonData. Результаты пишутся в `<reports-path>/raw/validation/`. При ошибке любой проверки job завершается с исключением.

### Проверки

| Проверка | Описание |
|---|---|---|
| `row_count_parity` | Число строк ORC == CarbonData |
| `checksum_parity` | Контрольные суммы по ключевым колонкам совпадают |
| `sample_query_parity` | Одинаковый фильтр возвращает одинаковый count в ORC и Carbon |
| `low_cardinality_bounds` | low cardinality колонки в ожидаемых пределах |
| `timestamp_range` | `timestamp` в заданном диапазоне |
| `log_format_distribution` | Все форматы логов присутствуют с ожидаемыми долями |
| `log_message_structure` | `log_message` не пустой, JSON-формат начинается с `{` |

### Параметры запуска

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `validate` |
| `--orc-path` | нет | `<base-path>/orc` | HDFS-путь к ORC |
| `--carbon-path` | нет | `<base-path>/carbon` | HDFS-путь к CarbonData |
| `--reports-path` | нет | `<base-path>/reports` | Путь для отчёта валидации |
| `--validation-checks` | нет | `all` | Список проверок или `all` |
| `--validation-sample-fraction` | нет | `0.01` | Доля выборки для проверок кардинальности/распределений |
| `--log-format-share-tolerance` | нет | `0.15` | Допустимое отклонение доли каждого `log_format` от равномерной |
| `--timestamp-start` | нет | `2024-01-01` | Ожидаемое начало диапазона `timestamp` |
| `--timestamp-end` | нет | `2025-01-01` | Ожидаемый конец диапазона `timestamp` |

### Unit-тесты

Локальные тесты без Spark-кластера:

```bash
./gradlew test
```

Покрывают: `ArgParser`, `GeneratorConfig`, `LogMessageBuilder`, `ValidationSettings`, `IndexProfile`.

### Пример запуска

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=validate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon \
  --validation-checks=all \
  --validation-sample-fraction=0.01
```

---

## Шаг 5. Формирование отчёта (`--mode=report`)

Агрегирует метрики из `<reports-path>/raw/` в `<reports-path>/summary/`.

> Статус: **реализован**.

### Параметры запуска

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `report` |
| `--reports-path` | нет | `<base-path>/reports` | HDFS-путь к отчётам |
| `--report-formats` | нет | `parquet,csv,json,markdown` | Форматы выходных отчётов |
| `--report-name` | нет | `benchmark-report` | Имя Markdown-отчёта (без расширения) |

### Выходные файлы (`<reports-path>/summary/`)

| Файл | Описание |
|---|---|---|
| `results.parquet` | Агрегированные метрики (benchmark, index, validation) |
| `results.csv` | То же в CSV |
| `results.json` | То же в JSON |
| `<report-name>.md` | Сводный Markdown: сравнение ORC vs Carbon, индексы, валидация, рекомендации |

Источники данных (читаются при наличии):
- `<reports-path>/raw/` — бенчмарки
- `<reports-path>/raw/index/` — индексные эксперименты
- `<reports-path>/raw/index/build-metrics/` — построение индексов
- `<reports-path>/raw/validation/` — валидация

### Пример запуска

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=report \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon
```

---

## Полный прогон пайплайна

Рекомендуемый порядок на кластере: **generate → validate → benchmark → index-experiment → report**.  
Перед ТБ-прогоном сделайте smoke с `--target-size-tb=0.01`.

```bash
JAR=build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar
BASE=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon

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

# 4. Индексные эксперименты
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=index-experiment \
  --base-path="$BASE" \
  --carbon-baseline-path="$BASE/carbon-baseline" \
  --carbon-bloom-path="$BASE/carbon-bloom" \
  --index-profiles=baseline,bloom

# 5. Отчёт
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=report \
  --base-path="$BASE"
```

---

## Ошибки валидации аргументов

| Ситуация | Сообщение об ошибке |
|---|---|---|
| Не передан `--mode` | `Missing required argument: --mode=...` |
| Неизвестный режим | `Unknown mode: <value>` |
| Неизвестный формат | `Unknown output format: <value>. Allowed: orc, carbon` |
| Нет форматов | `At least one output format must be enabled via --output-formats=orc,carbon` |
| Неверный формат аргумента | `Invalid argument: <arg>. Use --key=value` |
| Неположительное число | `Argument --<key> must be positive: <value>` |
| `timestamp-end` <= `timestamp-start` | `--timestamp-end must be greater than --timestamp-start` |
