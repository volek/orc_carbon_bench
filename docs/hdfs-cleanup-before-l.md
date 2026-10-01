# Очистка `layouts/*` перед Dataset L

Операционная инструкция к шагу README:

```text
# очистить лишние layouts/*, затем:
TARGET_SIZE_TB=0.1 … --layout=best_orc
TARGET_SIZE_TB=0.1 ./scripts/run-sla-matrix.sh
```

Контекст: полный порядок этапов — [cluster-run-protocol.md](cluster-run-protocol.md) (§2, §4 этап 9, §5).

Цель: освободить HDFS под generate L (~100 GB + temp), **предварительно** сохранив отчёты sweep/S для анализа.

---

## Что оставить / что удалить

| Оставить | Зачем |
|---|---|
| `layouts/best_orc` | единственный ORC для L / Hive / SLA |
| `layouts/b0` *(опционально)* | baseline для сравнения в анализе |

| Удалить | Типичные ID |
|---|---|
| Проигравшие factor-layout’ы после `SWEEP=audei` | `d0`, `d2`, `e2`, `e3`, `f0`, `f2`, `f3` (+ `d1` / `e1` / `f1`, если уже «свёрнуты» в `best_orc`) |
| Secondary (если гоняли) | `g*`, `h*`, `i*`, `j*`, `k*` |
| Spark-метки `s0` / `s1` | отдельных ORC нет — данные в `best_orc`; каталогов `layouts/s0` обычно нет |
| Legacy smoke | `$BASE/orc`, `$BASE/reports`, `$BASE/dictionary` (если ещё есть) |

**Правило перед L:** на диске только `best_orc` (+ опц. `b0`). Всё остальное в `layouts/*` — лишнее.

`-skipTrash` обязателен: без него место уйдёт в `.Trash` и не освободится сразу.

---

## Последовательность

### 0. Env

```bash
export BASE=hdfs:///user/hdfs_migration_user/orc_test
cd ~/orc-bench   # каталог со scripts/ и JAR
```

### 1. Инвентаризация

```bash
hdfs dfs -df -h
hdfs dfs -du -h -s "$BASE"/layouts/* 2>/dev/null
hdfs dfs -du -h -s "$BASE"/{orc,reports,dictionary} 2>/dev/null
hdfs dfs -ls "$BASE"/layouts/
```

Нужно ≥ **150–200 GB** free перед generate L (см. бюджет в [cluster-run-protocol.md](cluster-run-protocol.md) §1).

### 2. Выгрузить с HDFS **до** очистки

Без выгрузки summary/raw **до** `rm` сравнение факторов sweep будет потеряно. ORC losers копировать не нужно; markdown/parquet отчёты — нужно.

```bash
mkdir -p ~/orc-bench-results && cd ~/orc-bench-results

# Сводные markdown (главное для анализа)
hdfs dfs -get "$BASE"/reports/summary ./reports-summary 2>/dev/null || true
hdfs dfs -get "$BASE"/layouts/best_orc/reports/summary ./best_orc-summary
hdfs dfs -get "$BASE"/layouts/b0/reports/summary ./b0-summary 2>/dev/null || true

# Summary всех layout’ов sweep (пока они ещё на HDFS)
for id in $(hdfs dfs -ls "$BASE"/layouts 2>/dev/null | awk '{print $NF}' | xargs -n1 basename); do
  hdfs dfs -get "$BASE"/layouts/$id/reports/summary "./summary-$id" 2>/dev/null || true
done

# Raw parquet best_orc (p50/p95, sla_ok, scan_ratio)
hdfs dfs -get "$BASE"/layouts/best_orc/reports/raw ./best_orc-raw

# Dictionary (маленький; JOIN checks)
hdfs dfs -get "$BASE"/layouts/best_orc/dictionary ./best_orc-dictionary 2>/dev/null || true

# Hive CSV + concurrency + логи (уже на edge)
cp -a ~/orc-bench/result/concurrency ./concurrency 2>/dev/null || true
cp -a ~/orc-bench/result/hive ./hive 2>/dev/null || true
mkdir -p ./logs
cp -a ~/orc-bench/factor-*.log ~/orc-bench/smoke-*.log \
      ~/orc-bench/sla-*.log ~/orc-bench/layout-sweep-*.log ./logs 2>/dev/null || true
```

**Не** копировать: весь `layouts/*/orc` (десятки–сотни GB), `_temporary`, `.Trash`.  
По желанию: 1–2 part-файла из `best_orc/orc` для ручной проверки схемы.

Проверка выгрузки:

```bash
find ~/orc-bench-results -type f | head -50
du -sh ~/orc-bench-results
```

### 3. Очистить лишние layouts

Явный список (подставьте реальные ID из шага 1):

```bash
hdfs dfs -rm -r -skipTrash \
  "$BASE"/layouts/d0 "$BASE"/layouts/d1 "$BASE"/layouts/d2 \
  "$BASE"/layouts/e1 "$BASE"/layouts/e2 "$BASE"/layouts/e3 \
  "$BASE"/layouts/f0 "$BASE"/layouts/f1 "$BASE"/layouts/f2 "$BASE"/layouts/f3
  # + secondary / прочие losers при наличии

# Если b0 не нужен для сравнения:
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts/b0

# Legacy smoke-пути
hdfs dfs -rm -r -skipTrash "$BASE"/orc "$BASE"/reports "$BASE"/dictionary 2>/dev/null || true
```

Либо keep-list (оставить только нужное):

```bash
KEEP="best_orc b0"   # или KEEP="best_orc"
for path in $(hdfs dfs -ls "$BASE"/layouts 2>/dev/null | awk '{print $NF}'); do
  id=$(basename "$path")
  echo " $KEEP " | grep -q " $id " && continue
  echo "REMOVING $path"
  hdfs dfs -rm -r -skipTrash "$path"
done
```

Контроль:

```bash
hdfs dfs -du -h -s "$BASE"/layouts/*
hdfs dfs -df -h
```

### 4. Dataset L + SLA matrix

```bash
TARGET_SIZE_TB=0.1 SCENARIOS=audei CACHE_STATE=cold \
  ./scripts/run-factor.sh --layout=best_orc

TARGET_SIZE_TB=0.1 SCENARIOS=audei \
  ./scripts/run-sla-matrix.sh
# опционально archive suite: RUN_ST=1 ./scripts/run-sla-matrix.sh
```

Проверки:

```bash
hdfs dfs -du -h -s "$BASE"/layouts/best_orc/orc   # ≈ 100 GB
hdfs dfs -cat "$BASE"/reports/summary/sla-matrix-final.md 2>/dev/null | head -100
# ищите: epk_eq_14d, sla_class, sla_success, Validation PASS
```

### 5. После L — добрать финальные отчёты

```bash
cd ~/orc-bench-results
hdfs dfs -get "$BASE"/layouts/best_orc/reports/summary ./best_orc-summary-L
hdfs dfs -get "$BASE"/reports/summary ./reports-summary-L 2>/dev/null || true
hdfs dfs -get "$BASE"/layouts/best_orc/reports/raw ./best_orc-raw-L
```

Опционально освободить HDFS после сохранения отчётов:

```bash
# hdfs dfs -rm -r -skipTrash "$BASE"/layouts
# hdfs dfs -rm -r -skipTrash "$BASE"/orc "$BASE"/reports
```

---

## Краткая шпаргалка

1. `du` / `ls` по `layouts/*`
2. Скачать **все** `reports/summary` (+ `best_orc` raw, Hive/concurrency/logs)
3. Удалить всё в `layouts/*`, кроме `best_orc` (и опц. `b0`), с `-skipTrash`
4. `run-factor.sh --layout=best_orc` на `0.1` → `run-sla-matrix.sh`
5. Скачать L-отчёты
