# Сводный анализ результатов ORC-bench

Дата сборки отчёта: 2026-10-01. Источники: `orc-bench-results/` (выгрузка с HDFS) и `result/` (edge).

Suite: **audei** (`epk_eq_1d`, `epk_eq_14d`, `epk_page`, `eq_filters`, `order_by_epk_day`).
SLA interactive: **≤ 3000 ms**. Метрика latency: **p50_ms** (и p95 где критично).

Масштабы: **S** ≈ 20 GB (`TARGET_SIZE_TB=0.02`), **L** ≈ 100 GB (`0.1`).

---

## 1. Общая таблица прогонов

| # | Этап / тест | Суть | Масштаб / layout | Ключевой результат | Вердикт |
|---:|---|---|---|---|---|
| | Layout `b0` | Baseline ORC: без bloom, без sort; partition day | S / `b0` | geo-mean interactive p50 ≈ **633 ms**; epk_eq_14d p50=908 ms; bytes_read≈567 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `d0` | Partition: none (без партиций) | S / `d0` | geo-mean interactive p50 ≈ **659 ms**; epk_eq_14d p50=686 ms; bytes_read≈547 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `d1` | Partition: event_year/month/day | S / `d1` | geo-mean interactive p50 ≈ **747 ms**; epk_eq_14d p50=1188 ms; bytes_read≈567 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `d2` | Partition: year/month/day/hour | S / `d2` | geo-mean interactive p50 ≈ **2127 ms**; epk_eq_14d p50=4144 ms; bytes_read≈988 MB; SLA epk_eq_14d=0.0% | FAIL SLA (epk_* > 3s) |
| | Layout `e1` | Sort: epk_id | S / `e1` | geo-mean interactive p50 ≈ **719 ms**; epk_eq_14d p50=1094 ms; bytes_read≈461 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `e2` | Sort: event_id | S / `e2` | geo-mean interactive p50 ≈ **625 ms**; epk_eq_14d p50=837 ms; bytes_read≈461 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `e3` | Sort: event_ts + epk_id | S / `e3` | geo-mean interactive p50 ≈ **639 ms**; epk_eq_14d p50=1023 ms; bytes_read≈513 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `f0` | Bloom: none | S / `f0` | geo-mean interactive p50 ≈ **589 ms**; epk_eq_14d p50=902 ms; bytes_read≈567 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `f1` | Bloom: epk_id | S / `f1` | geo-mean interactive p50 ≈ **601 ms**; epk_eq_14d p50=695 ms; bytes_read≈96 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `f2` | Bloom: epk_id + event_id | S / `f2` | geo-mean interactive p50 ≈ **663 ms**; epk_eq_14d p50=885 ms; bytes_read≈567 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Layout `f3` | Bloom: epk_id + event_id + module | S / `f3` | geo-mean interactive p50 ≈ **633 ms**; epk_eq_14d p50=965 ms; bytes_read≈95 MB; SLA epk_eq_14d=100.0% | OK SLA |
| | Freeze `best_orc` + factor cold | day partition + sort epk_id + bloom epk_id | L / best_orc | geo-mean interactive p50 ≈ **542 ms**; epk_eq_14d p50=691 ms; SLA 100% по всем audei | **PASS** |
| | Spark S0 (toggles OFF) | ORC best_orc; Spark exec profile `s0` | S (отчёт также в L-папке — идентичен) | geo-mean interactive p50 ≈ **707 ms**; epk_eq_14d p50=1048 / p95=4184 | частичный FAIL (SLA epk_eq_14d=80.0%, p95=4184) |
| | Spark S1 (toggles ON) | ORC best_orc; Spark exec profile `s1` | S | geo-mean interactive p50 ≈ **618 ms**; epk_eq_14d p50=1124 / p95=1275 | PASS |
| | Spark S1 cold | ORC best_orc; Spark exec profile `s1` | L | geo-mean interactive p50 ≈ **560 ms**; epk_eq_14d p50=969 / p95=994 | PASS |
| | Spark S1 warm | ORC best_orc; Spark exec profile `s1` | L | geo-mean interactive p50 ≈ **537 ms**; epk_eq_14d p50=787 / p95=1002 | PASS |
| | Spark toggle `o1_cbo_on` | CBO ON поверх best_orc (отчёт перезаписал factor-best_orc на S) | S | geo-mean interactive p50 ≈ **581 ms**; epk_eq_14d p50=839 | PASS |
| | Hive `h0` — Hive Tez baseline (vec off) | Beeline → same `layouts/best_orc/orc` | best_orc (S/L CSV в result/) | geo-mean interactive p50 ≈ **20992 ms** (~21.0 s); SLA interactive **0%** | **FAIL SLA 3s** |
| | Hive `h1` — Hive Tez + vectorization | Beeline → same `layouts/best_orc/orc` | best_orc (S/L CSV в result/) | geo-mean interactive p50 ≈ **20631 ms** (~20.6 s); SLA interactive **0%** | **FAIL SLA 3s** |
| | Hive `h2` — Hive Tez + vectorization + CBO | Beeline → same `layouts/best_orc/orc` | best_orc (S/L CSV в result/) | geo-mean interactive p50 ≈ **20382 ms** (~20.4 s); SLA interactive **0%** | **FAIL SLA 3s** |
| | Hive `h3` — Hive LLAP cold | Beeline → same `layouts/best_orc/orc` | best_orc (S/L CSV в result/) | geo-mean interactive p50 ≈ **20250 ms** (~20.3 s); SLA interactive **0%** | **FAIL SLA 3s** |
| | Hive `h4` — Hive LLAP warm | Beeline → same `layouts/best_orc/orc` | best_orc (S/L CSV в result/) | geo-mean interactive p50 ≈ **20100 ms** (~20.1 s); SLA interactive **0%** | **FAIL SLA 3s** |
| | Concurrency 9 × Spark `epk_eq_14d` | 9 параллельных YARN application | best_orc warm | 9/9 SUCCEEDED; wall-clock app ≈ 25–149 s (p50≈97 s); **query durationMs в CSV пуст** | частичный OK (метрики query не собраны) |
| | Concurrency 18 × Spark `epk_eq_14d` | 18 параллельных submit | best_orc warm | **15/18 FAILED**: `No space left on device` при zip Spark libs на edge; 3 SUCCEEDED | **FAIL** |
| | Legacy `result/benchmark-report.md` | Старый suite (filter_*/full_scan/group_by), 1 run | неизвестен | p50 ≈ 180–194 **секунд** на full-scan сценариях | справочно, не AUDEI |

---

## 2. Layout sweep (Dataset S) — latency p50, ms

Холодный кэш, engine=spark, suite=audei.

| Layout | Суть | epk_eq_1d | epk_eq_14d | epk_page | eq_filters | order_by_epk_day | Geo-mean interactive | SLA epk_* |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `b0` | Baseline ORC: без bloom, без sort; partition day | 1219 | 908 | 826 | 176 | 144 | **633** | 100% |
| `d0` | Partition: none (без партиций) | 950 | 686 | 583 | 496 | 407 | **659** | 100% |
| `d1` | Partition: event_year/month/day | 1278 | 1188 | 890 | 231 | 171 | **747** | 100% |
| `d2` | Partition: year/month/day/hour | 5396 | 4144 | 3733 | 245 | 239 | **2127** | 0% (epk) |
| `e1` | Sort: epk_id | 1154 | 1094 | 1050 | 202 | 167 | **719** | 100% |
| `e2` | Sort: event_id | 1202 | 837 | 835 | 182 | 141 | **625** | 100% |
| `e3` | Sort: event_ts + epk_id | 1105 | 1023 | 795 | 185 | 147 | **639** | 100% |
| `f0` | Bloom: none | 1030 | 902 | 835 | 155 | 135 | **589** | 100% |
| `f1` | Bloom: epk_id | 1077 | 695 | 828 | 210 | 169 | **601** | 100% |
| `f2` | Bloom: epk_id + event_id | 1287 | 885 | 825 | 206 | 141 | **663** | 100% |
| `f3` | Bloom: epk_id + event_id + module | 1086 | 965 | 661 | 232 | 197 | **633** | 100% |

### Bytes read на `epk_eq_14d` (эффект pruning / bloom)

| Layout | avg_bytes_read | vs b0 |
|---|---:|---:|
| `b0` | 567.0 MB | 1.00× |
| `d0` | 547.3 MB | 0.97× |
| `d1` | 567.0 MB | 1.00× |
| `d2` | 988.2 MB | 1.74× |
| `e1` | 460.6 MB | 0.81× |
| `e2` | 460.6 MB | 0.81× |
| `e3` | 512.9 MB | 0.90× |
| `f0` | 566.9 MB | 1.00× |
| `f1` | 96.5 MB | 0.17× |
| `f2` | 566.9 MB | 1.00× |
| `f3` | 95.3 MB | 0.17× |

---

## 3. BEST_ORC / Spark matrix / Dataset L

| Профиль | Масштаб | cache | epk_eq_1d | epk_eq_14d | epk_page | eq_filters | order_by | Geo-mean int. | SLA |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| best_orc (generate+bench) | L | cold | 959 | 691 | 649 | 201 | 130 | **542** | 100% |
| s0 toggles OFF | S* | cold | 1160 | 1048 | 908 | 226 | 159 | **707** | 80.0% (p95=4184) |
| s1 toggles ON | S | cold | 1297 | 1124 | 686 | 146 | 142 | **618** | 100% |
| o1_cbo_on | S | cold | 1040 | 839 | 801 | 163 | 144 | **581** | 100% |
| s1 toggles ON | L | cold | 907 | 969 | 603 | 186 | 143 | **560** | 100% |
| s1 toggles ON | L | warm | 1081 | 787 | 548 | 178 | 141 | **537** | 100% |

\* Отчёт `factor-s0-cold.md` в `best_orc-summary-L` **байт-в-байт совпадает** с S-версией (bytes_read=574 MB). Отдельный L-прогон S0, скорее всего, не сохранился / был перезаписан — интерпретировать осторожно.

На L при best_orc/s1: **bytes_read ≈ 140 MB** на epk_* при датасете ~100 GB → сильный predicate/bloom pruning (scan_ratio ≪ 1%).

---

## 4. Hive H0–H4 (best_orc)

Порог SLA 3000 ms; warmup исключён. Источник: последние CSV в `result/hive/`.

| Профиль | Суть | Engine | cache | epk_eq_1d | epk_eq_14d | epk_page | eq_filters | order_by | Geo-mean int. | SLA 3s |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| `h0` | Hive Tez baseline (vec off) | hive_tez | cold | 22470 | 22625 | 23464 | 16279 | 15483 | **20992** | 0% |
| `h1` | Hive Tez + vectorization | hive_tez | cold | 22390 | 22056 | 22980 | 15963 | 16059 | **20631** | 0% |
| `h2` | Hive Tez + vectorization + CBO | hive_tez | cold | 21504 | 22376 | 22667 | 15824 | 17021 | **20382** | 0% |
| `h3` | Hive LLAP cold | hive_llap | cold | 21399 | 21585 | 22247 | 16365 | 15707 | **20250** | 0% |
| `h4` | Hive LLAP warm | hive_llap | warm | 21067 | 22053 | 22482 | 15628 | 16094 | **20100** | 0% |

> **Сноска:** таблица выше — **legacy** `SESSION_MODE=per_query` (wall-clock нового Beeline на каждый query ≈ 15–23 s). Это не HS2 query time. После перехода на `SESSION_MODE=persistent` (default) переснимите H0–H4; методика: [hive-latency-under-3s.md](hive-latency-under-3s.md).

Hive на тех же ORC в legacy-методике стабильно **~15–23 s** на запрос — на порядок медленнее Spark (~0.2–1.2 s). Разница H0→H4 в пределах шума (~5%); vectorization/CBO/LLAP warm **не** выводят interactive SLA в 3 s при per-query Beeline.

---

## 5. Concurrency 9 / 18

Сценарий: `epk_eq_14d`, engine=spark, layout=best_orc, cache=warm.

| Concurrency | Succ / Fail | Причина fail | Метрики query (`durationMs`) | Wall-clock app (оценка из логов) |
|---:|---|---|---|---|
| 9 | 9/9 SUCCEEDED, 0 NO_SPACE | — | пусто в `concurrency-spark.csv` (grep durationMs не нашёл) | n=9; p50≈97000 ms; min=25000; max=149000 |
| 18 | 3/18 SUCCEEDED, 15 NO_SPACE | 15 clients: local disk full при упаковке Spark jars (`java.io.IOException: No space left on device`) | пусто в `concurrency-spark.csv` (grep durationMs не нашёл) | n=3; p50≈48000 ms; min=26000; max=48000 |

Вывод: параллельный fan-out через N полных `spark-submit` на edge упирается в **локальный диск edge-ноды**, а не в ORC. Для QPS-теста нужен другой метод (меньше jar-upload / shared libs / меньше concurrent submits с одной ноды).

---

## 6. Выводы

### Layout (S)

1. **Победитель по interactive geo-mean:** `f0`/`f1`/`best_orc`-класс (~580–600 ms). Близки `e2`, `b0`, `f3`.
2. **Partition hour (`d2`) — антипаттерн** для AUDEI: epk_* p50 **3.7–5.4 s**, SLA 0%, bytes_read ≈ **1.7×** от b0 (много мелких файлов/партиций).
3. **Partition none (`d0`)** даёт быстрые epk_* (меньше planning overhead?), но **eq_filters/order_by деградируют** (496/407 ms vs ~170/140 у b0) из‑за отсутствия prune.
4. **Day partition (`d1`) — разумный дефолт**; hour не нужен.
5. **Bloom `epk_id` (`f1`)** режет I/O на epk_eq_14d: **~96 MB vs ~567 MB** у `f0`/`b0` (~6× меньше). Latency выигрыш скромнее (p50 695 vs 902), но pruning подтверждён.
6. Расширение bloom на `event_id`/`module` (`f2`/`f3`) **не улучшает** AUDEI geo-mean; `f2` даже читает снова ~567 MB (bloom не срабатывает на этом фильтре так же эффективно в прогоне).
7. Sort `epk_id` (`e1`) снижает bytes (~460 MB), но p50 не лучше `e2`/`b0` на этом suite — выигрыш скорее в стабильности I/O.

### BEST_ORC + Spark на L

8. На Dataset **L** `best_orc` cold: все interactive сценарии **уверенно внутри 3 s** (p50 201–959 ms, geo-mean ≈ **520 ms**).
9. **S1 vs S0:** S0 читает ~574 MB и имеет p95 epk_eq_14d **4184 ms** (SLA 80%); S1/best_orc — ~60–140 MB и SLA 100%. Spark ORC pushdown/vectorized/AQE **критичны**.
10. Warm vs cold на L/s1: небольшой выигрыш на `epk_page`/`eq_filters`; `epk_eq_1d` warm даже чуть хуже — кэш не меняет картину qualitatively.
11. Масштабирование S→L **не ломает** interactive SLA при best_orc+S1: latency того же порядка при росте данных ×5.

### Hive

12. Hive Tez/LLAP на тех же файлах в **legacy per_query**: **~20 s** vs Spark **~1 s** → не подходит для AUDEI interactive SLA 3 s при старой методике.
13. H0–H4 практически равны в legacy: узкое место — **startup/Beeline/Tez session**, а не ORC scan. После `SESSION_MODE=persistent` переснять; план снижения: [hive-latency-under-3s.md](hive-latency-under-3s.md). Interactive target по-прежнему **Spark**.

### Concurrency

14. Concurrency **9** формально прошла (YARN SUCCEEDED), но **нет валидных durationMs** — нельзя заявить sla_success.
15. Concurrency **18** провалилась из‑за **No space left on device** на edge при параллельной упаковке Spark dependencies — инфраструктурный лимит, не ORC.

### Рекомендации

| Решение | Рекомендация |
|---|---|
| ORC layout | **day partition + sort epk_id + bloom epk_id** (`best_orc`); **не** hour-partition |
| Spark | Держать **S1** (filter pushdown, vectorized, AQE, …) включённым |
| Interactive engine | **Spark 3.2**; Hive — только archive/batch или отдельный тюнинг |
| SLA на L | Spark best_orc **выполняет** interactive ≤3s на audei suite |
| Concurrency/QPS | Переработать методику (shared yarn.archive, меньше параллельных submit с одной edge, или long-lived Spark app) + свободное место на `/tmp` |
| Данные отчётов | Пересохранить «чистый» `factor-best_orc` на S (сейчас файл затёрт `o1_cbo_on`); перепрогнать S0 на L отдельно |

---

## 7. Источники

- Layout sweep: `orc-bench-results/summary-{b0,d0..f3}/factor-*-cold.md`
- L / Spark matrix: `orc-bench-results/best_orc-summary-L/`, `orc-bench-results/best_orc-summary/`
- Hive: `result/hive/hive-best_orc-h{0..4}-*.csv`
- Concurrency: `result/concurrency/`
- Протокол: [cluster-run-protocol.md](cluster-run-protocol.md), [hdfs-cleanup-before-l.md](hdfs-cleanup-before-l.md)

