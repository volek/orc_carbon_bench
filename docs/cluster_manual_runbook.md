# Ручной запуск и проверка на кластере

Кластер недоступен из среды разработки — прогон выполняется вручную с edge-ноды.  
Ниже: smoke ORC на кластерном Spark 3.2, сбор логов.

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

При известных квотах YARN добавьте флаги до `--`:

```bash
./scripts/submit-spark32.sh --driver-memory 8g --executor-memory 8g \
  --executor-cores 4 --num-executors 16 -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01
```

---

## 2. Smoke (обязательно первым)

Не используйте дефолт `--target-size-tb=5`. Smoke: `0.01`.

Короткий путь:

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
./scripts/run-smoke.sh
```

По шагам:

```bash
# 1. generate ORC
./scripts/submit-spark32.sh -- \
  --mode=generate --base-path="$BASE" --target-size-tb=0.01 --seed=42 \
  2>&1 | tee smoke-generate.log

# 2. validate
./scripts/submit-spark32.sh -- --mode=validate --base-path="$BASE" \
  2>&1 | tee smoke-validate.log

# 3. benchmark ORC
./scripts/submit-spark32.sh -- \
  --mode=benchmark --base-path="$BASE" \
  --benchmark-scenarios=all --benchmark-warmup-runs=1 --benchmark-repeat-runs=1 \
  2>&1 | tee smoke-benchmark.log

# 4. report
./scripts/submit-spark32.sh -- --mode=report --base-path="$BASE" \
  2>&1 | tee smoke-report.log
```

### Критерий успеха smoke

- Все job в статусе `SUCCEEDED`
- Есть пути: `$BASE/orc`, `$BASE/reports/raw/`, `$BASE/reports/summary/`
- Markdown-отчёт содержит секции Benchmark Summary и Validation

Рекомендуемый порядок: **generate → validate → benchmark → report**.

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

## 4. Pilot / full (после успешного smoke)

| Этап | `--target-size-tb` |
|---|---|
| Pilot | `0.1` … `0.5` |
| Full | по согласованию (дефолт приложения — `5`) |

Команды те же, что в smoke.

---

## 5. Сбор логов для анализа

```bash
APP_ID=application_XXXXXXXX_XXXX

mkdir -p ~/bench-logs && cd ~/bench-logs
cp ~/orc-bench/smoke-*.log . 2>/dev/null || true
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
