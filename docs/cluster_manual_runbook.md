# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Ниже: подготовка, smoke, сбор логов для анализа.

## Параметры кластера

| Параметр | Значение |
|---|---|
| HDFS namenode | `dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| HDFS URI | `hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| Spark | `3.2.1.3.5.7.0-1-SNAPSHOT` |
| Scala | `2.12.15` |
| JVM | OpenJDK `1.8.0_472` |
| Hadoop | `3.1.3.3.5.7.0-1-SNAPSHOT` |
| Артефакт | `orc-carbon-bench-0.1.0-SNAPSHOT-all.jar` (fat JAR, CarbonData внутри) |

```bash
export JAR=~/orc-carbon-bench/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar
export BASE=hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470/bench/orc-carbon
```

Если нет прав на `/bench/...`, используйте `/user/$USER/bench/orc-carbon`.

CarbonData уже в fat JAR — **не** передавайте `--packages`.

---

## 1. Подготовка на edge-ноде

Скопируйте fat JAR с машины сборки:

```bash
scp build/libs/orc-carbon-bench-0.1.0-SNAPSHOT-all.jar \
  user@edge-host:~/orc-carbon-bench/
```

На edge:

```bash
java -version          # ожидается 1.8.x
spark-submit --version # ожидается 3.2.1.x
hdfs dfs -ls "$BASE" || hdfs dfs -mkdir -p "$BASE"
```

При известных квотах YARN добавьте в `spark-submit`:

```text
--driver-memory … --executor-memory … --executor-cores … --num-executors …
```

---

## 2. Smoke (обязательно первым)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.

```bash
# generate ~0.01 TB
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=generate \
  --base-path="$BASE" \
  --target-size-tb=0.01 \
  --seed=42 \
  --output-formats=orc,carbon \
  --enable-bloom-index=true \
  --enable-lucene-index=true \
  2>&1 | tee smoke-generate.log

# validate
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=validate \
  --base-path="$BASE" \
  2>&1 | tee smoke-validate.log

# короткий benchmark
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=benchmark \
  --base-path="$BASE" \
  --benchmark-scenarios=full_scan,filter_high_cardinality \
  --benchmark-warmup-runs=1 \
  --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark.log

# report
spark-submit --master yarn --deploy-mode cluster \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=report \
  --base-path="$BASE" \
  2>&1 | tee smoke-report.log
```

### Критерий успеха smoke

- Все четыре job в статусе `SUCCEEDED`
- Есть пути: `$BASE/orc`, `$BASE/carbon`, `$BASE/reports/raw/`, `$BASE/reports/summary/`

Рекомендуемый порядок на кластере: **generate → validate → benchmark → index-experiment → report**.

---

## 3. Проверки после прогона

```bash
hdfs dfs -du -h -s "$BASE"/orc "$BASE"/carbon "$BASE"/reports
hdfs dfs -ls -R "$BASE"/reports/summary | head
yarn application -list -appStates FINISHED | head

# applicationId из yarn / spark-submit:
yarn logs -applicationId application_XXXXXXXX_XXXX > yarn-app.log
```

---

## 4. Pilot / full (после успешного smoke)

| Этап | `--target-size-tb` |
|---|---|
| Pilot | `0.1` … `0.5` |
| Full | по согласованию (дефолт приложения — `5`) |

Команды те же, что в smoke; для полного сравнения индексов генерируйте отдельные Carbon-пути (`carbon-baseline`, `carbon-bloom`, …) — см. [README.md](../README.md).

---

## 5. Сбор логов для анализа

Соберите архив и передайте разработчику (файл или ссылка).

```bash
APP_ID=application_XXXXXXXX_XXXX   # из упавшего или успешного job

mkdir -p ~/bench-logs && cd ~/bench-logs
cp ~/orc-carbon-bench/smoke-*.log . 2>/dev/null || true
yarn logs -applicationId "$APP_ID" > yarn-${APP_ID}.log 2>&1

# краткий статус HDFS
hdfs dfs -du -h -s "$BASE"/* > hdfs-du.txt 2>&1
hdfs dfs -ls -R "$BASE"/reports > hdfs-reports-ls.txt 2>&1

# summary с HDFS (если report прошёл)
hdfs dfs -get "$BASE"/reports/summary ./summary 2>&1 || true

# версии окружения
{ java -version; spark-submit --version; hadoop version; } > env-versions.txt 2>&1

tar -czf bench-logs.tgz smoke-*.log yarn-*.log hdfs-*.txt env-versions.txt summary 2>/dev/null
```

### Минимум при падении

1. Полная команда `spark-submit` и exit code
2. `yarn logs -applicationId …` (или driver stderr из YARN UI)
3. Вывод `java -version` и `spark-submit --version`
4. Текст exception / stack trace (обычно в конце yarn log)

### Минимум при успешном smoke

1. Файлы `smoke-*.log`
2. `hdfs-du.txt`
3. Каталог `summary/` (особенно `*.md`)
