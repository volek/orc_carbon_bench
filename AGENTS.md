# AGENTS.md

Этот репозиторий — **приложение для бенчмарков Spark 3 / Java 8** по сравнению форматов хранения ORC и CarbonData на HDFS.

## Быстрый старт

```bash
# Сборка (Windows)
gradlew.bat build

# Запуск на кластере dev1-abyss-sdp2-ambari-01
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain \
  build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  --mode=generate \
  --base-path=hdfs://dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470/bench/orc-carbon \
  --target-size-tb=5
```

## Критические ограничения

- **Только Java 8** — toolchain зафиксирован в `build.gradle`
- **Требуется Spark 3.2.x** — не меняйте версию Spark
- **Только fat JAR** — используйте `*-all.jar`, никогда не передавайте `--packages` (CarbonData встроен)
- **Требуется кластер HDFS** — локальный режим не поддерживается для основных этапов
- **Формат аргументов** — все параметры должны быть `--ключ=значение` (без пробелов и дефисов в ключах)

## Порядок этапов пайплайна

**Не пропускайте шаги** — выполняйте строго по порядку:

```text
generate → validate → benchmark → index-experiment → report
```

`validate` должен пройти успешно перед `benchmark`. `index-experiment` требует отдельных CarbonData-таблиц на каждый профиль (см. `--carbon-baseline-path`, `--carbon-bloom-path`, и т.д.)

## Артефакты сборки

| Файл | Назначение |
|------|-----------|
| `build/libs/orc-carbon-bench-0.1.0-SNAPSHOT.jar` | Тонкий JAR (используйте только для тестов) |
| `build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar` | **Fat JAR с CarbonData** — используйте для всех production-запусков |

`.gitignore` исключает `build/`, но **оставляет** `build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar`

## Команды для тестирования

| Команда | Назначение |
|---------|-----------|
| `./gradlew build` | Компиляция + shadowJar |
| `./gradlew test` | Запуск JUnit 5 тестов (без Spark-кластера) |

Тесты покрывают: `ArgParser`, `GeneratorConfig`, `LogMessageBuilder`, `ValidationSettings`, `IndexProfile`

## Особенности режимов

### `--mode=generate`
- `--output-formats=orc,carbon` записывает **одновременно** в оба формата
- Bloom/Lucene-индексы создаются **один раз** после всех чанков (задаются через `--enable-bloom-index`, `--enable-lucene-index`)
- По умолчанию `--target-size-tb=5` — для smoke-тестов используйте `0.01`

### `--mode=benchmark`
- Записывает метрики в `<reports-path>/raw/` как Parquet
- `--clear-cache-between-runs=true` (по умолчанию) — Spark-кэш очищается перед каждым запуском
- `--benchmark-warmup-runs=1` (по умолчанию) — прогревочные запуски не записываются в выход

### `--mode=index-experiment`
- Требуются **отдельные пути CarbonData** для каждого профиля (`baseline`, `bloom`, `lucene`, `bloom_lucene`)
- Lucene-поиск выполняется по каждому `log_format` (json, plain_text, key_value, apache_common)

### `--mode=validate`
- `--validation-checks=all` запускает 7 проверок на равенство/распределение
- Прерывает работу при любом несовпадении (работа завершается исключением)

### `--mode=report`
- Читает из `<reports-path>/raw/`, `<reports-path>/raw/index/`, `<reports-path>/raw/validation/`
- Вывод: Parquet, CSV, JSON и Markdown-сводка

## Матрица версий

```
Java:           8
Spark:          3.2.1 (компиляция)
CarbonData:     2.3.0 (org.apache.carbondata:carbondata-spark_3.1)
Scala (Spark):  2.12
Gradle:         shadow plugin 8.1.1
Тестирование:   JUnit 5 (BOM 5.10.2)
```

## Распространённые ошибки

- ❌ Использование тонкого JAR вместо fat JAR → `ClassNotFoundException` для CarbonData
- ❌ Передача `--packages org.apache.carbondata...` → конфликт версий
- ❌ Запуск `benchmark` без предварительного `validate` → неизвестна целостность данных
- ❌ Повторное использование одного CarbonData-пути для всех профилей индексов → некорректное сравнение
- ❌ Использование Spark 3.3+ → несовместимость API (сборка зафиксирована на 3.2.1)

## Примечания по кластеру

**HDFS**: `dev1-abyss-sdp2-ambari-01.opsmon.sbt:50470`  
**Порядок этапов**: `generate → validate → benchmark → index-experiment → report`  
**Smoke-тест**: `--target-size-tb=0.01`  
**Полный пример**: см. `RUNNING_ON_CLUSTER.md`
