# orc-carbon-bench

Spark / Java 8 приложение для сравнения форматов хранения ORC и CarbonData на HDFS.

Проект рассчитан на кластер **без изменений**: SDP Spark 3.2 остаётся как есть. CarbonData и основное сравнение ORC vs Carbon идут на **своём Spark 3.1.1** (BYOS на YARN). Кластерный Spark 3.2 используется только как референс ORC.

## Требования

- Java 8 (совместимо с JVM кластера)
- Hadoop / YARN / HDFS кластера (не меняем)
- Hive Metastore как уже существующий сервис (нужен CarbonData)
- Fat JAR `orc-carbon-bench-spark31-all.jar` — приложение + CarbonData 2.3.0 / Spark 3.1
- Fat JAR `orc-carbon-bench-spark32-all.jar` — приложение без CarbonData, ORC-референс на Spark 3.2
- Архив Apache Spark **3.1.1** — в git как `dist/spark-3.1.1-bin-without-hadoop.tgz.part-*` (сплит из‑за лимита GitHub 100 МБ); на edge **не** скачивается

Не сабмитьте spark31-JAR через кластерный `spark-submit` 3.2: на classpath окажется Spark 3.2, и CarbonData 2.3.0 (`carbondata-spark_3.1`) будет несовместим.

## Сборка

```bash
./gradlew build
```

Windows:

```bash
gradlew.bat build
```

Артефакты (копируются в `build/libs/`):
- `orc-carbon-bench-spark31-all.jar` — Spark 3.1.1 + CarbonData 2.3.0 (модуль `carbondata-spark_3.1`), без Spark/Hadoop
- `orc-carbon-bench-spark32-all.jar` — Spark 3.2 ORC-only, без CarbonData

В `dist/` лежат части дистрибутива Spark 3.1.1 (`spark-3.1.1-bin-without-hadoop.tgz.part-*`) для edge.

`./gradlew build` скачивает полный архив Spark только если его ещё нет в `dist/`, затем режет на части <100 МБ. На edge интернет для Apache не нужен: `prepare-spark31.sh` склеивает `.part-*` и распаковывает.

Версии сборки: Spark compile `3.1.1` (модуль `app-spark31`) и `3.2.1` (модуль `app-spark32`).

Локальные unit-тесты:

```bash
./gradlew test
```

## Два рантайма

| Рантайм | Submit | JAR | Режимы |
|---|---|---|---|
| Spark 3.1.1 BYOS | `./scripts/submit-spark31.sh` | `orc-carbon-bench-spark31-all.jar` | `generate`, `validate`, `benchmark`, `index-experiment`, `report` |
| Spark 3.2 кластера | `./scripts/submit-spark32.sh` | `orc-carbon-bench-spark32-all.jar` | `benchmark --formats=orc`, опционально `generate --output-formats=orc`, `report` |

Подготовка Spark 3.1.1 на edge (один раз, **без скачивания**):

```bash
# git clone уже содержит dist/*.tgz.part-*; либо scp с машины сборки:
scp dist/spark-3.1.1-bin-without-hadoop.tgz.part-* \
    build/libs/orc-carbon-bench-spark31-all.jar \
    build/libs/orc-carbon-bench-spark32-all.jar \
    user@edge-host:~/orc-carbon-bench/
scp -r scripts user@edge-host:~/orc-carbon-bench/

# на edge: склейка частей + распаковка + hive-site.xml
# если скрипты приехали с Windows: sed -i 's/\r$//' scripts/*.sh
mkdir -p dist
mv -n spark-3.1.1-bin-without-hadoop.tgz.part-* dist/ 2>/dev/null || true
./scripts/prepare-spark31.sh
```

Скрипт склеивает `spark-3.1.1-bin-without-hadoop.tgz.part-*` (лимит GitHub 100 МБ), распаковывает в `dist/spark-3.1.1/` и копирует клиентский `hive-site.xml`. `SPARK_CONF_DIR` кластерного Spark 3.2 **не** наследуется. Если частей нет — ошибка, а не попытка скачать с Apache.

`submit-spark31.sh` выставляет `SPARK_DIST_CLASSPATH=$(hadoop classpath)` для варианта without-hadoop.

Флаги `spark-submit` передаются до `--`, аргументы приложения — после:

```bash
./scripts/submit-spark31.sh --driver-memory 8g --num-executors 16 -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

## Формат аргументов

Все параметры приложения передаются в формате `--ключ=значение`.

- Каждый аргумент должен начинаться с `--` и содержать `=`.
- Ключ и значение не могут быть пустыми.
- Регистр значения `--mode` не важен (`generate` и `GENERATE` эквивалентны).

## Обзор пайплайна

```text
generate (spark31) → validate (spark31) → benchmark (spark31)
  → index-experiment (spark31, Bloom/Lucene) → benchmark ORC (spark32) → report (spark31)
```

| Шаг | `--mode` | Рантайм | Выход | Статус |
|---|---|---|---|---|
| 1. Генерация данных | `generate` | spark31 | `<orc-path>/`, `<carbon-path>/` | реализован |
| 2. Валидация | `validate` | spark31 | `<reports-path>/raw/validation/` | реализован |
| 3. Бенчмарки ORC+Carbon | `benchmark` | spark31 | `<reports-path>/raw/` | реализован |
| 4. Индексные эксперименты | `index-experiment` | spark31 | `<reports-path>/raw/index/` | реализован |
| 5. Референс ORC | `benchmark --formats=orc` | spark32 | `<reports-path>/raw/spark32-orc/` | реализован |
| 6. Отчёт | `report` | spark31 | `<reports-path>/summary/` | реализован |

Промежуточный слой Parquet **не используется** — данные генерируются и сразу записываются в ORC и CarbonData. Референс Spark 3.2 читает **тот же** `--orc-path`.

### Конфигурируемые пути HDFS

Все пути задаются независимо. Если явный путь не указан, он вычисляется от `--base-path`:

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--base-path` | `hdfs:///user/hdfs_migration_user/carbon_test` | Корневой путь эксперимента |
| `--orc-path` | `<base-path>/orc` | Путь для ORC-данных |
| `--carbon-path` | `<base-path>/carbon` | Путь для CarbonData |
| `--reports-path` | `<base-path>/reports` | Путь для отчётов бенчмарков |

Пример для целевого кластера:

```bash
--base-path=hdfs:///user/hdfs_migration_user/carbon_test \
--orc-path=hdfs:///user/hdfs_migration_user/carbon_test/orc \
--carbon-path=hdfs:///user/hdfs_migration_user/carbon_test/carbon \
--reports-path=hdfs:///user/hdfs_migration_user/carbon_test/reports
```

Структура каталогов:

```text
<orc-path>/          # ORC-файлы (пишет Spark 3.1.1, читает и 3.2)
<carbon-path>/       # CarbonData-таблица
<reports-path>/
  raw/               # сырые метрики spark31
  raw/spark32-orc/   # референс ORC на Spark 3.2
  raw/index/         # индексные эксперименты
  raw/validation/    # валидация
  summary/           # агрегированные отчёты
```

Raw parquet содержит поля `spark_version` и `spark_runtime` (`spark31-carbon` / `spark32-orc`).

### Общие параметры (все шаги)

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | Режим: `generate`, `validate`, `benchmark`, `index-experiment`, `report` |
| `--base-path` | нет | `hdfs:///user/hdfs_migration_user/carbon_test` | Корневой путь (используется для вычисления путей по умолчанию) |
| `--orc-path` | нет | `<base-path>/orc` | Абсолютный HDFS-путь для ORC |
| `--carbon-path` | нет | `<base-path>/carbon` | Абсолютный HDFS-путь для CarbonData |
| `--reports-path` | нет | `<base-path>/reports` | Абсолютный HDFS-путь для отчётов |

---

## Шаг 1. Генерация данных (`--mode=generate`)

Генерирует синтетический датасет и **сразу записывает** его в ORC и/или CarbonData. Промежуточный Parquet не создаётся.

По умолчанию пишет в оба формата (`--output-formats=orc,carbon`). На spark32-артефакте допустим только `--output-formats=orc`.

### Параметры запуска

#### Общие

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `generate` |
| `--base-path` | нет | `hdfs:///user/hdfs_migration_user/carbon_test` | Корневой путь |
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

ORC-настройки выставляются в runtime (`spark.conf().set`).  
CarbonData-расширения задаются на `SparkSession.Builder` до `getOrCreate()`. `submit-spark31.sh` уже передаёт:

```bash
--conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions \
--conf spark.sql.session.state.builder=org.apache.spark.sql.hive.CarbonSessionStateBuilder
```

CarbonData уже внутри spark31 fat JAR — **не** передавайте `--packages`.

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

Полная генерация 5 ТБ в ORC и CarbonData (Spark 3.1.1):

```bash
./scripts/submit-spark31.sh -- \
  --mode=generate \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --target-size-tb=5 \
  --seed=42 \
  --output-formats=orc,carbon \
  --orc-compression=snappy \
  --enable-bloom-index=true \
  --enable-lucene-index=true
```

Только ORC:

```bash
./scripts/submit-spark31.sh -- \
  --mode=generate \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --output-formats=orc \
  --target-size-tb=1 \
  --orc-compression=zstd
```

Только CarbonData с индексами:

```bash
./scripts/submit-spark31.sh -- \
  --mode=generate \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --output-formats=carbon \
  --target-size-tb=1 \
  --enable-bloom-index=true \
  --bloom-index-columns=user_id,product_id,event_id \
  --enable-lucene-index=true
```

---

## Шаг 2. Бенчмарки (`--mode=benchmark`)

Запускает тесты производительности на данных из `<orc-path>/` и `<carbon-path>/`.

- spark31 пишет в `<reports-path>/raw/` (`spark_runtime=spark31-carbon`)
- spark32 пишет в `<reports-path>/raw/spark32-orc/` (`spark_runtime=spark32-orc`)

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

### Метрики (Parquet)

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
| `spark_version` | `spark.version()` |
| `spark_runtime` | `spark31-carbon` или `spark32-orc` |

Прогрев (`--benchmark-warmup-runs`) выполняется без записи в отчёт. При `--clear-cache-between-runs=true` кэш Spark очищается перед каждым измеряемым прогоном.

### Параметры запуска

| Параметр | Обязательный | По умолчанию | Описание |
|---|---|---|---|
| `--mode` | да | — | `benchmark` |
| `--base-path` | нет | `hdfs:///user/hdfs_migration_user/carbon_test` | Корневой путь |
| `--orc-path` | нет | `<base-path>/orc` | HDFS-путь к ORC-данным |
| `--carbon-path` | нет | `<base-path>/carbon` | HDFS-путь к CarbonData |
| `--reports-path` | нет | `<base-path>/reports` | HDFS-путь для отчётов |
| `--seed` | нет | `42` | Seed для воспроизводимости фильтров |
| `--benchmark-warmup-runs` | нет | `1` | Прогревочные запуски |
| `--benchmark-repeat-runs` | нет | `3` | Измеряемые повторы |
| `--benchmark-scenarios` | нет | `all` | Сценарии через запятую или `all` |
| `--clear-cache-between-runs` | нет | `true` | Очистка кэша между прогонами |
| `--formats` | нет | `orc,carbon` | Форматы для сравнения; на spark32 только `orc` |

### Пример запуска

Spark 3.1.1, ORC + Carbon:

```bash
./scripts/submit-spark31.sh -- \
  --mode=benchmark \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --seed=42
```

Референс ORC на кластерном Spark 3.2 (тот же `--orc-path`):

```bash
./scripts/submit-spark32.sh -- \
  --mode=benchmark \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --formats=orc \
  --seed=42
```

---

## Шаг 3. Индексные эксперименты (`--mode=index-experiment`)

Только spark31. Сравнивает ORC (reference) с CarbonData в профилях `baseline`, `bloom`, `lucene`, `bloom_lucene`. Запускает индексные сценарии и Lucene-поиск в разрезе каждого `log_format`.

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
./scripts/submit-spark31.sh -- \
  --mode=index-experiment \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --carbon-baseline-path=hdfs:///user/hdfs_migration_user/carbon_test/carbon-baseline \
  --carbon-bloom-path=hdfs:///user/hdfs_migration_user/carbon_test/carbon-bloom \
  --carbon-lucene-path=hdfs:///user/hdfs_migration_user/carbon_test/carbon-lucene \
  --carbon-bloom-lucene-path=hdfs:///user/hdfs_migration_user/carbon_test/carbon-bloom-lucene \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --seed=42
```

---

## Шаг 4. Валидация данных (`--mode=validate`)

Только spark31. Проверяет корректность и согласованность данных в ORC и CarbonData. Результаты пишутся в `<reports-path>/raw/validation/`. При ошибке любой проверки job завершается с исключением.

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

### Пример запуска

```bash
./scripts/submit-spark31.sh -- \
  --mode=validate \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test \
  --validation-checks=all \
  --validation-sample-fraction=0.01
```

---

## Шаг 5. Формирование отчёта (`--mode=report`)

Агрегирует метрики из `<reports-path>/raw/` и `<reports-path>/raw/spark32-orc/` в `<reports-path>/summary/`.

Markdown содержит:
1. ORC vs Carbon на Spark 3.1.1
2. ORC Spark 3.1.1 vs ORC Spark 3.2
3. Index experiments (Bloom / Lucene), build metrics, validation

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
| `<report-name>.md` | Сводный Markdown: сравнение форматов, движков, индексы, валидация, рекомендации |

### Пример запуска

```bash
./scripts/submit-spark31.sh -- \
  --mode=report \
  --base-path=hdfs:///user/hdfs_migration_user/carbon_test
```

---

## Полный прогон пайплайна

Рекомендуемый порядок на кластере: **generate → validate → benchmark (3.1.1) → index-experiment → benchmark ORC (3.2) → report**.  
Перед ТБ-прогоном сделайте smoke: `./scripts/run-smoke.sh` (`--target-size-tb=0.01`).

```bash
export BASE=hdfs:///user/hdfs_migration_user/carbon_test

./scripts/prepare-spark31.sh

./scripts/submit-spark31.sh -- --mode=generate --base-path="$BASE" \
  --target-size-tb=5 --seed=42 --enable-bloom-index=true --enable-lucene-index=true

./scripts/submit-spark31.sh -- --mode=validate --base-path="$BASE"

./scripts/submit-spark31.sh -- --mode=benchmark --base-path="$BASE"

./scripts/submit-spark31.sh -- --mode=index-experiment --base-path="$BASE" \
  --carbon-baseline-path="$BASE/carbon-baseline" \
  --carbon-bloom-path="$BASE/carbon-bloom" \
  --carbon-lucene-path="$BASE/carbon-lucene" \
  --carbon-bloom-lucene-path="$BASE/carbon-bloom-lucene" \
  --index-profiles=baseline,bloom,lucene,bloom_lucene

./scripts/submit-spark32.sh -- --mode=benchmark --base-path="$BASE" --formats=orc

./scripts/submit-spark31.sh -- --mode=report --base-path="$BASE"
```

Подробности кластерного запуска — в [docs/cluster_manual_runbook.md](docs/cluster_manual_runbook.md).

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
| Carbon-режим на spark32 JAR | `CarbonData and index experiments require orc-carbon-bench-spark31-all.jar ...` |
