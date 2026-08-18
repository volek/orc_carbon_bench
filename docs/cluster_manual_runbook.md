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

Статические Carbon-конфиги (Spark 3.2 не даёт менять их после создания сессии):

```bash
export CARBON_CONF="--conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog"
```

```bash
# generate ~0.01 TB
spark-submit --master yarn --deploy-mode cluster \
  $CARBON_CONF \
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
  $CARBON_CONF \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=validate \
  --base-path="$BASE" \
  2>&1 | tee smoke-validate.log

# короткий benchmark
spark-submit --master yarn --deploy-mode cluster \
  $CARBON_CONF \
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

---

## 6. Типичные ошибки submit (разбор логов)

Ошибки ниже относятся к **инфраструктуре кластера**, не к коду `orc-carbon-bench`.  
`AppMain` / `--mode=generate` стартуют только после успешного submit и выдачи токенов.

### 6.1. YARN ResourceManager: `Connection refused` на `:8032`

**Симптом:** `ConfiguredRMFailoverProxyProvider` / `Call From … to …:8032 failed … Connection refused`.

**Смысл:** клиент не достучался до YARN RM (сервис down, неверный host/port, firewall, оба RM в HA недоступны).

**Что делать:**

```bash
yarn node -list
yarn rmadmin -getAllServiceState
grep -E 'yarn.resourcemanager\.(address|ha|hostname)' /etc/hadoop/conf/yarn-site.xml
```

Поднять ResourceManager / NodeManager в Ambari или поправить `yarn-site.xml`.

---

### 6.2. SSL: `SSLContext does not support … algorithms: sdp-deployer`

**Симптом:** после upload JAR в staging:

```text
IllegalArgumentException: requirement failed: SSLContext does not support any of the enabled algorithms: sdp-deployer
```

**Смысл:** в Spark SSL-конфиге (`spark.ssl.*` / Ambari) указано значение `sdp-deployer` вместо валидных TLS cipher suites.

**Что делать:** найти и исправить в `$SPARK_HOME/conf` / Ambari:

```bash
grep -rniE 'ssl|sdp-deployer|enabledAlgorithms' $SPARK_HOME/conf /etc/spark*/conf 2>/dev/null
```

Убрать `sdp-deployer` из списка algorithms или отключить ненужный `spark.ssl.enabled`. Правка платформы, не приложения.

---

### 6.3. Hive Metastore недоступен (`:9083`)

**Симптом:**

```text
Trying to connect to metastore with URI thrift://…ambari-02:9083
Trying to connect to metastore with URI thrift://…ambari-03:9083
Failed to connect to the MetaStore Server...
Unable to instantiate … SessionHiveMetaStoreClient
```

И подсказка Spark:

```text
If hive is not used, set spark.security.credentials.hive.enabled to false
```

**Смысл:** при submit Spark пытается взять Hive delegation token; Metastore на `:9083` не слушает.

**Что делать (предпочтительно):** в Ambari поднять **Hive Metastore** (Healthy на обоих URI из `hive-site.xml`).

Проверка:

```bash
nc -vz dev1-abyss-sdp2-ambari-02.opsmon.sbt 9083
nc -vz dev1-abyss-sdp2-ambari-03.opsmon.sbt 9083
```

Для полного Carbon (`CarbonSessionCatalog`, индексы) живой Metastore обычно **нужен**.

---

### 6.4. HBase RegionServer: `Connection refused` на `:16020`

**Симптом:** после (или вместо) Hive-токенов:

```text
Call to address=…flink-01.opsmon.sbt:16020 failed … Connection refused
… on table 'hbase:meta' …
RpcRetryingCallerImpl: Call exception, tries=N, retries=36 …
```

Submit **зависает** на ретраях (минуты); `AppMain` не запускается.

**Смысл:** на classpath есть HBase-клиент (`/usr/sdp/current/hbase-client/...`); Spark берёт HBase credentials, ZK отдаёт RS `…flink-01:16020`, процесс не слушает.

**Что делать (предпочтительно):** поднять **HBase** (Master + RegionServer) или поправить локацию RS в конфиге/ZK.

Проверка:

```bash
nc -vz dev1-abyss-sdp2-flink-01.opsmon.sbt 16020
echo "status" | hbase shell
```

---

### 6.5. Временный обход: отключить Hive/HBase credentials

Если сервисы не поднять сразу, а нужно только пройти submit / ORC-smoke:

```bash
spark-submit --master yarn --deploy-mode cluster \
  --conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog \
  --conf spark.security.credentials.hive.enabled=false \
  --conf spark.security.credentials.hbase.enabled=false \
  --class ru.sber.orcbench.AppMain "$JAR" \
  --mode=generate \
  --base-path="$BASE" \
  --target-size-tb=0.01 \
  --seed=42 \
  --output-formats=orc,carbon \
  --enable-bloom-index=true \
  --enable-lucene-index=true \
  2>&1 | tee smoke-generate.log
```

**Ограничения:** это снимает зависание на токенах. Если при записи Carbon job всё равно обратится к Metastore — снова понадобится живой Hive (п. 6.3). Для полного сравнения ORC vs Carbon предпочтителен п. A (поднять сервисы).

---

### 6.6. Чеклист перед повторным smoke

1. `yarn node -list` — есть RUNNING NodeManager’ы  
2. Нет ошибки `sdp-deployer` в SSL  
3. Hive Metastore отвечает на `:9083` **или** задано `spark.security.credentials.hive.enabled=false`  
4. HBase RS отвечает на `:16020` **или** задано `spark.security.credentials.hbase.enabled=false`  
5. В логе после submit есть строки приложения (`orc-carbon-bench` / `Writing` / `Generating`), а не только ретраи HBase  

При новом падении прислать полный `smoke-*.log` + `applicationId` + `yarn logs -applicationId …`.

---

### 6.7. `Invalid numeric argument for --target-size-tb: 0.01`

**Симптом:** AM стартует, затем:

```text
IllegalArgumentException: Invalid numeric argument for --target-size-tb: 0.01
NumberFormatException: For input string: "0.01"
```

**Смысл:** старые сборки парсили `--target-size-tb` как `long`. Нужен fat JAR с поддержкой дробных ТБ (`double`).

**Что делать:** использовать актуальный `orc-carbon-bench-0.1.0-SNAPSHOT-all.jar` (после фикса) и снова `--target-size-tb=0.01`.

---

### 6.8. `element_at` … data type mismatch: `[array<string>, bigint]`

**Симптом:** generate падает в `DataGenerator.generateChunk`:

```text
AnalysisException: cannot resolve 'element_at(array(...), (pmod(...) + 1))'
Input to function element_at should have been array followed by a int, but it's [array<string>, bigint]
```

**Смысл:** в Spark 3.2 индекс для `element_at` должен быть **int**, а генератор передавал **bigint** (результат `pmod` от `global_id`).

**Что делать:** использовать fat JAR с фиксом (`.cast(IntegerType)` для индекса в `elementAtDictionary`). Обхода через CLI нет.

---

### 6.9. `Cannot modify the value of a static config: spark.sql.extensions`

**Симптом:** AM стартует (часто RUNNING → ACCEPTED → RUNNING — рестарт AM), затем:

```text
AnalysisException: Cannot modify the value of a static config: spark.sql.extensions
  at CarbonWriter.setIfMissing
  at SparkConfigurator.configure
  at AppMain.main
```

**Смысл:** в Spark 3.2 `spark.sql.extensions` нельзя менять через `spark.conf().set()` после создания `SparkSession`. Старые JAR пытались выставить CarbonExtensions в runtime.

**Что делать:**

1. Использовать fat JAR, который задаёт Carbon-конфиги на `SparkSession.Builder` до `getOrCreate()`.
2. На submit всё равно передать (на случай, если контекст уже создан платформой):

```bash
--conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog
```

---

### 6.10. `CarbonSource could not be instantiated` / `CarbonStreamException`

**Симптом:** generate доходит до записи (часто даже `.orc()`), затем:

```text
ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister:
  Provider org.apache.spark.sql.CarbonSource could not be instantiated
Caused by: ClassNotFoundException: org.apache.carbondata.streaming.CarbonStreamException
```

**Смысл:** Spark при любом `DataFrameWriter.save` поднимает все SPI `DataSourceRegister`. `CarbonSource` ссылается на класс из модуля `carbondata-streaming`. Старые fat JAR исключали этот модуль (~52 KB) — падала даже запись ORC.

**Что делать:** использовать fat JAR, куда снова включён `carbondata-streaming_3.1` (без Spark Streaming / Kafka). Обхода через CLI нет.

---

### 6.11. `VerifyError` в `CarbonSecondaryIndexOptimizer` (Spark 3.2)

**Симптом:** generate падает при первой записи (часто ORC), после загрузки `CarbonExtensions`:

```text
VerifyError: Bad type on operand stack
  CarbonSecondaryIndexOptimizer.createIndexFilterDataFrame
  Type UnaryNode is not assignable to LogicalPlan
  at CarbonSITransformationRule.<init>
  at CarbonOptimizer.defaultBatches
```

**Смысл:** `carbondata-spark_3.1:2.3.0` собран под Spark 3.1; на кластере Spark 3.2.1 байткод SI optimizer не проходит verification. CarbonOptimizer подключается ко **всем** запросам, включая `.orc()`.

**Что делать:** fat JAR с no-op shim-классом `CarbonSecondaryIndexOptimizer` (legacy SI rewrite отключён; Bloom/Lucene индексы работают). Обхода через CLI нет.

**Ограничение:** secondary-index plan rewrite CarbonData на Spark 3.2 не используется — для bench это не требуется.
