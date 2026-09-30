# Операционный протокол прогона на кластере

Пошаговый порядок тестов AUDEI/ORC-bench: параметры, очистка HDFS, проверки успеха, что забирать с HDFS.

Связанные документы: [cluster_manual_runbook.md](cluster_manual_runbook.md), [audei-st-workload-mapping.md](audei-st-workload-mapping.md), [README.md](../README.md).

---

## 1. Предусловия (один раз)

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export NUM_EXECUTORS=16 EXECUTOR_MEMORY=8g EXECUTOR_CORES=4 DRIVER_MEMORY=4g
cd ~/orc-bench   # каталог со scripts/ и JAR
```

| Проверка | Команда | Ожидание |
|---|---|---|
| JAR на месте | `ls -lh "$JAR"` | `orc-bench-all.jar` |
| Доступ к HDFS | `hdfs dfs -ls /user/hdfs_migration_user` | без ошибок |
| Свободное место | `hdfs dfs -df -h` | см. таблицу бюджета ниже |
| YARN | `yarn node -list` / UI ResourceManager | есть READY-ноды |
| Права на BASE | `hdfs dfs -mkdir -p "$BASE"` | OK или уже существует |

### Бюджет места перед стартом

| Этап | Сколько свободно нужно минимум | Почему |
|---|---|---|
| Smoke | ≥ **30 GB** | ~10 GB data + temp + reports |
| Layout sweep S (`SWEEP=audei`, ~10 layout’ов) | ≥ **250–300 GB** | каждый layout ≈ 20 GB |
| BEST_ORC + S0/S1 + Hive (без generate) | ≥ **50 GB** сверх `best_orc` | reports + temp |
| Dataset L generate | ≥ **150–200 GB** | 100 GB data + temp + запас |
| Concurrency 9/18 | запас YARN-очереди; HDFS почти не растёт | только reports |

```bash
hdfs dfs -df -h
hdfs dfs -du -h -s "$BASE" 2>/dev/null || echo "BASE пуст — нормально для первого прогона"
```

---

## 2. Нужно ли чистить HDFS?

| Момент | Чистить? | Что именно |
|---|---|---|
| **Перед первым прогоном** | Да, если старые эксперименты | весь `$BASE` или только `layouts/*`, `orc`, `reports` |
| **Перед smoke** | Желательно пустой/чистый BASE | иначе смешаются старые reports |
| **Между layout-ами в sweep** | Не обязательно | каждый layout в своём каталоге |
| **После выбора победителя фактора** | **Да** | удалить проигравшие `layouts/<id>` |
| **Перед Dataset L** | **Да** | оставить только `best_orc` (+ при необходимости `b0`) |
| **После generate FAILED** (DN disk / temp) | **Да** | `layouts/<id>/orc/_temporary`, неполный `orc/` |
| **После успешного всего цикла** | По желанию | ORC можно удалить; **reports сохранить/скачать** |
| **Между cold и warm** | Нет (данные те же) | только cache protocol Spark/LLAP |

### Команды очистки

```bash
# Инвентаризация
hdfs dfs -du -h -s "$BASE"/layouts/* 2>/dev/null
hdfs dfs -du -h -s "$BASE"/{orc,reports,dictionary} 2>/dev/null

# Полный сброс эксперимента (осторожно)
# hdfs dfs -rm -r -skipTrash "$BASE"

# Только legacy smoke-пути (без layouts)
# hdfs dfs -rm -r -skipTrash "$BASE"/orc "$BASE"/reports "$BASE"/dictionary

# Удалить проигравшие layout’ы (пример)
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/d0 "$BASE"/layouts/e2 "$BASE"/layouts/f0

# Сломанный generate
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/best_orc/orc/_temporary
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/best_orc/orc
```

Trash: `-skipTrash` экономит место; без флага данные уйдут в `.Trash` и место не освободится сразу.

---

## 3. Порядок этапов (канонический)

Не пропускайте проверки «успех этапа» — иначе следующий этап опирается на битые данные.

```text
0. Проверка HDFS/YARN
1. Smoke (audei, 10 GB)
2. Baseline B0 (S, 20 GB)
3. Layout sweep AUDEI (S) → выбрать победителей → чистка losers
4. Freeze best_orc (S)
5. Spark S0/S1 + toggles (без generate)
6. Hive H0–H4 (те же ORC)
7. Concurrency 9/18 на epk_eq_14d
8. [опц.] Dataset M — 2–3 кандидата
9. Чистка → Dataset L best_orc → SLA matrix
10. Скачать reports с HDFS; опционально удалить ORC
```

---

## 4. Этапы: команды, параметры, проверки

### Этап 0 — ресурсы

```bash
hdfs dfs -df -h
yarn application -list 2>/dev/null | head
```

**Успех:** Capacity Remaining ≳ бюджета этапа; нет зависших Application’ов, блокирующих очередь.

---

### Этап 1 — Smoke

```bash
# Перед: желательно чистый BASE или отдельный BASE_SMOKE
./scripts/run-smoke.sh
# параметры по умолчанию:
#   TARGET_SIZE_TB=0.01  BENCHMARK_SCENARIOS=audei
#   sort/bloom=epk_id  partition=year,month,day
```

| Проверка | Как | OK если |
|---|---|---|
| YARN jobs | логи `smoke-*.log`, YARN UI | все `SUCCEEDED` |
| Данные | `hdfs dfs -ls "$BASE"/orc` | есть part-файлы, `_SUCCESS` |
| Validation | `hdfs dfs -ls "$BASE"/reports/raw/validation` | parquet есть; в summary Validation PASS |
| Benchmark | `hdfs dfs -ls "$BASE"/reports/raw/benchmark` | parquet; сценарии `epk_eq_*` |
| Report | `hdfs dfs -cat "$BASE"/reports/summary/*.md \| head -80` | есть SLA / summary |

**Не** делайте выводы по layout/SLA на smoke.  
**Очистка после smoke (перед factor):** да — удалите `$BASE/orc` и `$BASE/reports`, либо используйте другой `BASE` для factor (`…/orc_test` vs `…/orc_test_smoke`).

---

### Этап 2 — Baseline B0 (Dataset S)

```bash
TARGET_SIZE_TB=0.02 SCENARIOS=audei CACHE_STATE=cold \
  ./scripts/run-factor.sh --layout=b0
```

| Проверка | OK если |
|---|---|
| Путь | `$BASE/layouts/b0/orc` непустой |
| Validate | log `factor-b0-validate.log` без `Validation failed` |
| Report | `$BASE/layouts/b0/reports/summary/factor-b0-cold.md` |
| Метрики | есть `sla_ok`, `sla_class`, `scan_ratio` |

**Очистка:** не трогать `b0`, пока не сравнили с sweep.

---

### Этап 3 — Layout sweep AUDEI (S)

```bash
# Перед: свободно ≳ 250 GB
hdfs dfs -df -h

TARGET_SIZE_TB=0.02 SCENARIOS=audei CACHE_STATE=cold \
  SWEEP=audei ./scripts/run-layout-sweep.sh
# = b0,d0,d1,d2,e1,e2,e3,f0,f1,f2,f3
```

После каждой группы (или всего sweep):

```bash
hdfs dfs -du -h -s "$BASE"/layouts/*
# сводный report
hdfs dfs -ls "$BASE"/reports/summary/   # или layouts/*/reports/summary/
```

**Успех этапа:** для каждого layout есть `orc/` + markdown report; job’ы SUCCEEDED.

**Очистка после выбора победителей (обязательно перед M/L):**

```bash
# Пример: оставили d1, e1, f1, b0 — остальное снести
hdfs dfs -rm -r -skipTrash \
  "$BASE"/layouts/d0 "$BASE"/layouts/d2 \
  "$BASE"/layouts/e2 "$BASE"/layouts/e3 \
  "$BASE"/layouts/f0 "$BASE"/layouts/f2 "$BASE"/layouts/f3
```

Вторичные факторы (по желанию, после чистки):

```bash
SWEEP=secondary TARGET_SIZE_TB=0.02 SCENARIOS=audei ./scripts/run-layout-sweep.sh
```

---

### Этап 4 — Freeze BEST_ORC (S)

```bash
TARGET_SIZE_TB=0.02 SCENARIOS=audei CACHE_STATE=cold \
  ./scripts/run-factor.sh --layout=best_orc
# default best_orc: day partition + sort epk_id + bloom epk_id @ FPP 0.01
```

| Проверка | OK если |
|---|---|
| ORC | `$BASE/layouts/best_orc/orc` ≈ 20 GB |
| Dictionary | `$BASE/layouts/best_orc/dictionary` |
| Report | `factor-best_orc-cold.md` |

**Очистка:** можно удалить промежуточные layout’ы, **кроме** `best_orc` (и опционально `b0` для baseline-сравнения).

---

### Этап 5 — Spark exec matrix (без generate)

```bash
# Перед: best_orc уже есть; SKIP_GENERATE=1 внутри скрипта
TARGET_SIZE_TB=0.02 SCENARIOS=audei \
  ./scripts/run-spark-exec-matrix.sh
```

**Успех:** reports с метками `s0`/`s1` и toggles; `orc/` не пересоздавался (время generate ≈ 0 в логах).

**Очистка HDFS данных:** нет.

---

### Этап 6 — Hive H0–H4

```bash
SUITE=audei LAYOUT=best_orc ./scripts/hive/run-hive-factor.sh h0
SUITE=audei LAYOUT=best_orc ./scripts/hive/run-hive-factor.sh h1
# … h2, h3 (LLAP cold), h4 (LLAP warm)
```

**Успех:** CSV/логи Beeline без ошибок; external table читает тот же `layouts/best_orc/orc`.

**Очистка:** нет (те же файлы).

---

### Этап 7 — Concurrency 9 / 18

```bash
ENGINE=spark LAYOUT=best_orc CACHE_STATE=warm \
  SCENARIO=epk_eq_14d CONCURRENCY_LEVELS="9 18" \
  ./scripts/run-concurrency.sh
```

**Успех:** `result/concurrency/concurrency-spark.csv` заполнен; `sla_success` считается по 3000 ms.

**Очистка HDFS:** нет. Локальный артефакт уже в `result/concurrency/` на edge.

---

### Этап 8 — (опционально) Dataset M

```bash
# Только 2–3 кандидата, не весь sweep
TARGET_SIZE_TB=0.05 SCENARIOS=audei CACHE_STATE=cold \
  ./scripts/run-factor.sh --layout=best_orc
```

---

### Этап 9 — Dataset L + SLA matrix

```bash
# 1) Проверить место
hdfs dfs -df -h
hdfs dfs -du -h -s "$BASE"/layouts/*

# 2) Оставить только нужное (минимум best_orc)
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/<losers>...

# 3) Generate L
TARGET_SIZE_TB=0.1 SCENARIOS=audei CACHE_STATE=cold \
  ./scripts/run-factor.sh --layout=best_orc

# 4) Cold + warm Spark S1 + Hive + report
TARGET_SIZE_TB=0.1 SCENARIOS=audei \
  ./scripts/run-sla-matrix.sh
# опционально archive suite: RUN_ST=1 ./scripts/run-sla-matrix.sh
```

| Проверка | OK если |
|---|---|
| Размер ORC | `hdfs dfs -du -h -s "$BASE"/layouts/best_orc/orc` ≈ 100 GB |
| SLA report | `$BASE/reports/summary/sla-matrix-final.md` (и layouts/…) |
| Interactive | `epk_eq_*` / `eq_filters`: смотреть `sla_success` при пороге 3 с |
| Archive | при `RUN_ST=1` — `like_*` с порогом 120 с |

---

## 5. Что забирать с HDFS после тестов

Скачивайте **отчёты и метрики**, не обязательно сырые ORC (сотни GB).

### Обязательно (результаты эксперимента)

```bash
mkdir -p ~/orc-bench-results && cd ~/orc-bench-results

# Markdown отчёты (главное для анализа)
hdfs dfs -get "$BASE"/reports/summary ./reports-summary 2>/dev/null || true
hdfs dfs -get "$BASE"/layouts/best_orc/reports/summary ./best_orc-summary
hdfs dfs -get "$BASE"/layouts/b0/reports/summary ./b0-summary 2>/dev/null || true

# Raw parquet benchmark/validation (для доп. анализа в Spark/Python)
hdfs dfs -get "$BASE"/layouts/best_orc/reports/raw ./best_orc-raw

# Hive CSV (если писались локально скриптом — уже на edge)
# иначе ищите путь в логе run-hive-factor

# Concurrency (локально на edge)
cp -a ~/orc-bench/result/concurrency ./concurrency 2>/dev/null || true

# Логи edge
cp -a ~/orc-bench/factor-*.log ~/orc-bench/smoke-*.log ~/orc-bench/sla-*.log ./logs 2>/dev/null || true
```

### По желанию

| Путь | Зачем |
|---|---|
| `layouts/*/reports/summary/*.md` | сравнение факторов |
| `layouts/best_orc/dictionary` | маленький; JOIN checks |
| Сэмпл 1–2 ORC part-файла | ручная проверка схемы (`orc-tools` / Spark) |

### Обычно не копировать

- весь `layouts/*/orc` (десятки–сотни GB)
- `_temporary`, `.Trash`

### После скачивания отчётов

```bash
# Освободить HDFS (если цикл завершён и отчёты сохранены локально)
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts
# hdfs dfs -rm -r -skipTrash "$BASE"/orc "$BASE"/reports
```

---

## 6. Поэтапный чеклист «всё ли прошло»

После **каждого** этапа:

1. **YARN:** Application `SUCCEEDED` (не `FAILED` / `KILLED`).
2. **Лог скрипта:** нет `Validation failed`, `Refusing TARGET_SIZE_TB`, `Exception`.
3. **HDFS пути существуют** (таблица ниже).
4. **Markdown:** секции Benchmark Summary / SLA / Validation заполнены.
5. **Место:** `hdfs dfs -df -h` — Remaining не упал до нуля; нет DN «out of space» в логах.

| Этап | Ключевые пути на HDFS |
|---|---|
| Smoke | `$BASE/orc`, `$BASE/reports/raw/{benchmark,validation}`, `$BASE/reports/summary/` |
| Factor layout X | `$BASE/layouts/X/orc`, `…/dictionary`, `…/reports/raw/*`, `…/reports/summary/` |
| Hive | те же `layouts/best_orc/orc`; метрики — CSV/edge |
| SLA final | `$BASE/reports/summary/sla-matrix-final.md` |

Быстрая проверка отчёта:

```bash
hdfs dfs -cat "$BASE"/layouts/best_orc/reports/summary/*.md 2>/dev/null | head -100
# ищите: epk_eq_14d, sla_class, sla_success, Validation PASS
```

Признаки **провала** generate (нужна чистка и повтор):

- в логе: `minimum replication`, `nodes excluded`, `DiskError`, `No space`
- на HDFS: `orc/_temporary` без нормальных part-файлов / нет `_SUCCESS`

---

## 7. Краткая шпаргалка «с нуля до L»

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
export JAR=~/orc-bench/orc-bench-all.jar
export NUM_EXECUTORS=16 EXECUTOR_MEMORY=8g

hdfs dfs -df -h

# 1 smoke
./scripts/run-smoke.sh
# почистить smoke-пути или сменить BASE перед factor

# 2–3 S
TARGET_SIZE_TB=0.02 SCENARIOS=audei ./scripts/run-factor.sh --layout=b0
TARGET_SIZE_TB=0.02 SCENARIOS=audei SWEEP=audei ./scripts/run-layout-sweep.sh
# → выбрать победителей, удалить losers

# 4–7
TARGET_SIZE_TB=0.02 SCENARIOS=audei ./scripts/run-factor.sh --layout=best_orc
./scripts/run-spark-exec-matrix.sh
SUITE=audei ./scripts/hive/run-hive-factor.sh h0
CONCURRENCY_LEVELS="9 18" SCENARIO=epk_eq_14d ./scripts/run-concurrency.sh

# 8–9 L
hdfs dfs -du -h -s "$BASE"/layouts/*
# rm losers…
TARGET_SIZE_TB=0.1 SCENARIOS=audei ./scripts/run-factor.sh --layout=best_orc
TARGET_SIZE_TB=0.1 ./scripts/run-sla-matrix.sh

# 10 скачать reports (см. §5)
```
