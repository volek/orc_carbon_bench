# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Ниже: подготовка BYOS Spark 3.1.1, smoke (Carbon + ORC + Bloom/Lucene + референс Spark 3.2), сбор логов.

Кластер **не меняем**: SDP Spark 3.2 остаётся установленным. Spark 3.1.1 едет с edge на YARN вместе с job.

## Параметры кластера

| Параметр | Значение |
|---|---|
| HDFS namenode | `dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| HDFS URI | `hdfs://dev1-abyss-sdp2-ambari-02.opsmon.sbt:50470` |
| Spark кластера (референс ORC) | `3.2.1.3.5.7.0-1-SNAPSHOT` |
| Spark BYOS (Carbon + ORC) | Apache `3.1.1` в `dist/spark-3.1.1/` |
| Scala | `2.12.x` |
| JVM | OpenJDK `1.8.0_472` |
| Hadoop | `3.1.3.3.5.7.0-1-SNAPSHOT` |
| Артефакты | `orc-carbon-bench-spark31-all.jar`, `orc-carbon-bench-spark32-all.jar` |
| BASE | `hdfs:///user/hdfs_migration_user/carbon_test` |

```bash
export BASE=hdfs:///user/hdfs_migration_user/carbon_test
export JAR31=~/orc-carbon-bench/orc-carbon-bench-spark31-all.jar
export JAR32=~/orc-carbon-bench/orc-carbon-bench-spark32-all.jar
```

Путь использует default FS из `core-site.xml` (`hdfs:///...`).

CarbonData уже в spark31 fat JAR — **не** передавайте `--packages`.

Не используйте кластерный `spark-submit` 3.2 для Carbon-джобов.

---

## 1. Подготовка на edge-ноде

Скопируйте оба fat JAR с машины сборки:

```bash
scp build/libs/orc-carbon-bench-spark31-all.jar \
    build/libs/orc-carbon-bench-spark32-all.jar \
    dist/spark-3.1.1-bin-without-hadoop.tgz.part-* \
  user@edge-host:~/orc-carbon-bench/
scp -r scripts user@edge-host:~/orc-carbon-bench/
```

На edge:

```bash
cd ~/orc-carbon-bench
java -version          # ожидается 1.8.x
spark-submit --version # кластерный Spark 3.2.1.x — только для ORC-референса
hdfs dfs -ls "$BASE" || hdfs dfs -mkdir -p "$BASE"

sed -i 's/\r$//' scripts/*.sh   # если скрипты приехали с Windows CRLF
chmod +x scripts/*.sh
mkdir -p dist
mv -n spark-3.1.1-bin-without-hadoop.tgz.part-* dist/ 2>/dev/null || true

./scripts/prepare-spark31.sh
# ожидание: dist/spark-3.1.1/bin/spark-submit и hive-site.xml в conf/
```

`scripts/prepare-spark31.sh`:
- **не** качает Spark с Apache (на edge интернета к archive.apache.org нет);
- склеивает `spark-3.1.1-bin-without-hadoop.tgz.part-*` (GitHub ≤100 МБ) и распаковывает из `dist/` или `build/libs/`;
- копирует клиентский `hive-site.xml` и **вырезает** `hadoop.security.credential.provider.path` (ссылка на `hive-site.jceks`, к которому у edge-пользователя часто нет прав);
- **не** выставляет `SPARK_CONF_DIR` на конфиг SDP Spark 3.2 (`spark.yarn.archive` иначе подменит Spark).

`submit-spark31.sh` дополнительно сбрасывает `spark.hadoop.hadoop.security.credential.provider.path`, если свойство всё ещё приходит из `HADOOP_CONF_DIR`.

`submit-spark31.sh` берёт Hadoop-клиент кластера через `SPARK_DIST_CLASSPATH=$(hadoop classpath)`.

При известных квотах YARN добавьте флаги до `--`:

```bash
./scripts/submit-spark31.sh --driver-memory 8g --executor-memory 8g \
  --executor-cores 4 --num-executors 16 -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

---

## 2. Smoke (обязательно первым)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.

Короткий путь:

```bash
export BASE=hdfs:///user/hdfs_migration_user/carbon_test
export JAR31=~/orc-carbon-bench/orc-carbon-bench-spark31-all.jar
export JAR32=~/orc-carbon-bench/orc-carbon-bench-spark32-all.jar
./scripts/run-smoke.sh
```

По шагам:

```bash
# 1. generate ORC + Carbon + Bloom + Lucene (Spark 3.1.1)
./scripts/submit-spark31.sh -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 --seed=42 \
  --output-formats=orc,carbon --enable-bloom-index=true --enable-lucene-index=true \
  2>&1 | tee smoke-generate.log

# 2. validate
./scripts/submit-spark31.sh -- --mode=validate --base-path="$BASE" \
  2>&1 | tee smoke-validate.log

# 3. benchmark ORC + Carbon на 3.1.1
./scripts/submit-spark31.sh -- \
  --mode=benchmark --base-path="$BASE" \
  --benchmark-scenarios=all --benchmark-warmup-runs=1 --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark.log

# 4. Bloom / Lucene index experiment
./scripts/submit-spark31.sh -- \
  --mode=index-experiment --base-path="$BASE" --rebuild-indexes=true \
  --index-profiles=baseline,bloom,lucene,bloom_lucene \
  --benchmark-warmup-runs=1 --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-index.log

# 5. ORC reference on cluster Spark 3.2 (тот же --orc-path)
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" --formats=orc \
  --benchmark-scenarios=all --benchmark-warmup-runs=1 --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark-spark32.log

# 6. combined report
./scripts/submit-spark31.sh -- --mode=report --base-path="$BASE" \
  2>&1 | tee smoke-report.log
```

### Критерий успеха smoke

- Все job в статусе `SUCCEEDED`
- Есть пути: `$BASE/orc`, `$BASE/carbon`, `$BASE/reports/raw/`, `$BASE/reports/raw/spark32-orc/`, `$BASE/reports/summary/`
- Markdown-отчёт содержит секции ORC vs Carbon (3.1.1), ORC 3.1.1 vs ORC 3.2, Index Experiments

Рекомендуемый порядок: **generate → validate → benchmark (3.1.1) → index-experiment → benchmark ORC (3.2) → report**.

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
{
  java -version
  spark-submit --version
  "$HOME/orc-carbon-bench/dist/spark-3.1.1/bin/spark-submit" --version
  hadoop version
} > env-versions.txt 2>&1

tar -czf bench-logs.tgz smoke-*.log yarn-*.log hdfs-*.txt env-versions.txt summary 2>/dev/null
```

### Минимум при падении

1. Полная команда `spark-submit` / `./scripts/submit-spark31.sh` и exit code
2. `yarn logs -applicationId …` (или driver stderr из YARN UI)
3. Вывод `java -version`, кластерный `spark-submit --version`, BYOS `$SPARK31_HOME/bin/spark-submit --version`
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

**Что делать:** не наследовать `SPARK_CONF_DIR` кластерного Spark 3.2 в BYOS 3.1.1 (`submit-spark31.sh` делает `unset SPARK_CONF_DIR`). Если ошибка на spark32-сабмите — править платформенный SSL, не приложение.

---

### 6.2a. `hive-site.jceks` Permission denied / Configuration problem with provider path

**Симптом:** после upload JAR в staging, до старта AM:

```text
java.io.IOException: Configuration problem with provider path.
Caused by: java.io.FileNotFoundException:
  /usr/sdp/current/hive-client/conf/hive-site.jceks (Permission denied)
```

**Смысл:** в `hive-site.xml` указан `hadoop.security.credential.provider.path` на `.jceks`, к которому у пользователя сабмита нет чтения. Spark `SecurityManager` / `SSLOptions` падает при `getPassword`.

**Что делать:** перезапустить `./scripts/prepare-spark31.sh` (sanitize BYOS `conf/hive-site.xml`) и сабмитить через `./scripts/submit-spark31.sh` (сбрасывает provider path из Hadoop conf). Системный `/etc/hive/conf` не трогаем. Альтернатива — выдать чтение на `.jceks` через админов SDP.

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

**Что делать (предпочтительно):** в Ambari поднять **Hive Metastore** (Healthy на обоих URI из `hive-site.xml`). Это существующий сервис кластера, не установка Spark 3.1.1.

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
./scripts/submit-spark31.sh \
  --conf spark.security.credentials.hive.enabled=false \
  --conf spark.security.credentials.hbase.enabled=false -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 \
  --output-formats=orc,carbon --enable-bloom-index=true --enable-lucene-index=true \
  2>&1 | tee smoke-generate.log
```

**Ограничения:** это снимает зависание на токенах. Если при записи Carbon job всё равно обратится к Metastore — снова понадобится живой Hive (п. 6.3). Для полного сравнения ORC vs Carbon предпочтителен п. A (поднять сервисы).

---

### 6.6. Чеклист перед повторным smoke

1. `yarn node -list` — есть RUNNING NodeManager’ы  
2. Нет ошибки `sdp-deployer` в SSL  
3. Hive Metastore отвечает на `:9083` **или** задано `spark.security.credentials.hive.enabled=false`  
4. HBase RS отвечает на `:16020` **или** задано `spark.security.credentials.hbase.enabled=false`  
5. BYOS: `$SPARK31_HOME/bin/spark-submit --version` показывает 3.1.1, `SPARK_CONF_DIR` не указывает на SDP Spark 3.2  
6. В логе после submit есть строки приложения (`orc-carbon-bench` / `Writing` / `Generating`), а не только ретраи HBase  

При новом падении прислать полный `smoke-*.log` + `applicationId` + `yarn logs -applicationId …`.

---

### 6.7. `Invalid numeric argument for --target-size-tb: 0.01`

**Симптом:** AM стартует, затем:

```text
IllegalArgumentException: Invalid numeric argument for --target-size-tb: 0.01
NumberFormatException: For input string: "0.01"
```

**Смысл:** старые сборки парсили `--target-size-tb` как `long`. Нужен актуальный fat JAR с поддержкой дробных ТБ (`double`).

---

### 6.8. `element_at` … data type mismatch: `[array<string>, bigint]`

**Симптом:** generate падает в `DataGenerator.generateChunk`:

```text
AnalysisException: cannot resolve 'element_at(array(...), (pmod(...) + 1))'
Input to function element_at should have been array followed by a int, but it's [array<string>, bigint]
```

**Смысл:** индекс для `element_at` должен быть **int**. Актуальный генератор делает `.cast(IntegerType)`.

---

### 6.9. `Cannot modify the value of a static config: spark.sql.extensions`

**Симптом:** AM стартует, затем:

```text
AnalysisException: Cannot modify the value of a static config: spark.sql.extensions
  at CarbonWriter.setIfMissing
  at SparkConfigurator.configure
  at AppMain.main
```

**Смысл:** `spark.sql.extensions` нельзя менять через `spark.conf().set()` после создания `SparkSession`.

**Что делать:** использовать `./scripts/submit-spark31.sh` (конфиги задаются на Builder и в `--conf` до `getOrCreate()`).

**Не используйте** `spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog` — такого plugin-класса нет (см. §6.12).

---

### 6.10. `CarbonSource could not be instantiated` / `CarbonStreamException`

**Симптом:** generate доходит до записи (часто даже `.orc()`), затем:

```text
ServiceConfigurationError: org.apache.spark.sql.sources.DataSourceRegister:
  Provider org.apache.spark.sql.CarbonSource could not be instantiated
Caused by: ClassNotFoundException: org.apache.carbondata.streaming.CarbonStreamException
```

**Смысл:** Spark при любом `DataFrameWriter.save` поднимает все SPI `DataSourceRegister`. Нужен spark31 fat JAR, куда включён `carbondata-streaming_3.1` (без Spark Streaming / Kafka).

---

### 6.11. `VerifyError` в `CarbonSecondaryIndexOptimizer` (сабмит spark31-JAR через Spark 3.2)

**Симптом:**

```text
VerifyError: Bad type on operand stack
  CarbonSecondaryIndexOptimizer.createIndexFilterDataFrame
  Type UnaryNode is not assignable to LogicalPlan
```

**Смысл:** `carbondata-spark_3.1:2.3.0` собран под Spark 3.1. Кластерный `spark-submit` 3.2 кладёт Spark 3.2 на classpath. No-op shim больше не используется.

**Что делать:** Carbon-джобы запускать только через `./scripts/submit-spark31.sh` (BYOS Spark 3.1.1). Для ORC-референса на кластерном 3.2 — только `orc-carbon-bench-spark32-all.jar` без CarbonData.

---

### 6.12. `Cannot find catalog plugin class … CarbonSessionCatalog`

**Симптом:**

```text
SparkException: Cannot find catalog plugin class for catalog 'spark_catalog':
  org.apache.spark.sql.CarbonSessionCatalog
  at OrcWriter.write / DataFrameWriter.orc
```

**Смысл:** в submit передали неверный `--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.CarbonSessionCatalog`. Это **не** V2 catalog plugin (реальный класс — `org.apache.spark.sql.hive.CarbonHiveSessionCatalog`, подключается через `CarbonSessionStateBuilder`).

**Что делать:** убрать `spark.sql.catalog.spark_catalog=…` и использовать `./scripts/submit-spark31.sh`.

---

### 6.13. Carbon-режим на spark32 JAR

**Симптом:**

```text
IllegalStateException: CarbonData and index experiments require orc-carbon-bench-spark31-all.jar
```

**Смысл:** `validate`, `index-experiment`, `--output-formats=carbon`, `--formats=carbon` запрещены на spark32-артефакте.

**Что делать:** эти режимы — только `submit-spark31.sh`. Для spark32: `--mode=benchmark --formats=orc`.
