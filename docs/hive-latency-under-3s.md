# Рекомендации: Hive latency &lt; 3 с (AUDEI interactive)

Контекст: по результатам прогонов H0–H4 (`result/hive/`, сводка в [benchmark-results-analysis.md](benchmark-results-analysis.md)) Hive на тех же ORC, что и Spark, стабильно давал **~15–23 s** на audei-запрос при SLA interactive **≤ 3000 ms** — в **legacy** режиме `SESSION_MODE=per_query` (новый Beeline на каждый query + wall-clock CLI).

Связанные документы: [cluster-run-protocol.md](cluster-run-protocol.md), [cluster_manual_runbook.md](cluster_manual_runbook.md), runner [`scripts/hive/run-hive-factor.sh`](../scripts/hive/run-hive-factor.sh).

---

## Реализация в репозитории (статус)

| Фаза | Артефакт | Статус |
|---|---|---|
| 0 Диагностика session tax | [`scripts/hive/diagnose-session-tax.sh`](../scripts/hive/diagnose-session-tax.sh) | сделано |
| 1 Persistent session + `Time taken` | `SESSION_MODE=persistent` (default) в `run-hive-factor.sh`; [`parse-beeline-timings.py`](../scripts/hive/parse-beeline-timings.py) | сделано |
| 2 Partition prune + projection | predicates в runner; [`queries_audei_st.sql`](../scripts/hive/queries_audei_st.sql); [`explain-audei.sh`](../scripts/hive/explain-audei.sh) | сделано |
| 3 Профили + LLAP preflight | h1/h2/h2f/h3/h4 SET’ы; [`llap-preflight.sh`](../scripts/hive/llap-preflight.sh) | сделано |
| 4 Docs / dual-SLA | этот файл + protocol / runbook | сделано |

Общие хелперы: [`scripts/hive/hive-lib.sh`](../scripts/hive/hive-lib.sh).

---

## Диагноз (legacy)

В старом runner каждый запрос поднимал **новый Beeline** и мерял полный wall-clock (~20 с). H0→H4 отличались на ~5% → узкое место не ORC scan.

---

## Как гонять (после изменений)

### 0) Session-tax A/B

```bash
PROFILE=h2 REPEATS=10 ./scripts/hive/diagnose-session-tax.sh
# → result/hive/session-tax-h2-*.csv
# verdict=SESSION_TAX | NO_SESSION_TAX | HIGH_LATENCY_STABLE
```

### 1) Factor (честный замер HS2)

```bash
# default: SESSION_MODE=persistent → duration_ms = «Time taken» из Beeline
SUITE=audei ./scripts/hive/run-hive-factor.sh h2

# legacy wall-clock (сравнение с историческими ~20 s)
SESSION_MODE=per_query SUITE=audei ./scripts/hive/run-hive-factor.sh h2
```

В логе ищем `METRIC hive_time_taken` и `wall_suite_ms=…` (весь suite одним Beeline).

### 2) EXPLAIN / prune

```bash
PROFILE=h2 ./scripts/hive/explain-audei.sh
# → result/hive/explain-h2-*.txt
```

### 3) Профили

| ID | Смысл |
|---|---|
| `h0` | Tez baseline (vec/CBO off) |
| `h1` | + vectorization + PPD/index + tez reuse |
| `h2` | + CBO + stats |
| `h2f` | h2 + `hive.fetch.task.conversion=more` (**не** смешивать с Tez/LLAP SLA) |
| `h3` | LLAP cold (нужен live daemon) |
| `h4` | LLAP warm (warmup в той же persistent-сессии) |

```bash
./scripts/hive/llap-preflight.sh          # перед h3/h4
SUITE=audei ./scripts/hive/run-hive-factor.sh h4
# обход: SKIP_LLAP_PREFLIGHT=1 (результаты не считать истинным LLAP)
```

---

## Приоритеты (напоминание)

1. **Убрать session tax** — persistent session + метрика `Time taken` (сделано в runner).
2. **Живой LLAP** — preflight + операторский старт daemon (чеклист ниже).
3. **SQL prune / projection** — partition predicates + без `payload_json` в page/order (сделано).
4. **SET knobs** — в профилях h1+ (сделано); `h2f` отдельно.
5. **Архитектура:** interactive AUDEI → **Spark**; Hive — archive/ST или post-tune после новых прогонов.

### LLAP: чеклист оператора

- [ ] `llapstatus` / YARN app LLAP в `RUNNING`
- [ ] Queue/ресурсы под LLAP на DN
- [ ] h4: warmup в той же сессии (runner делает это сам в persistent)
- [ ] Не интерпретировать h3/h4 при `SKIP_LLAP_PREFLIGHT=1` как LLAP SLA

---

## Интерпретация метрик

| Режим | `duration_ms` | Когда использовать |
|---|---|---|
| `SESSION_MODE=persistent` (default) | HS2 **Time taken** | SLA / сравнение с 3 с |
| `SESSION_MODE=per_query` | wall-clock Beeline process | A/B со старыми отчётами (~20 s) |

Исторические H0–H4 в [benchmark-results-analysis.md](benchmark-results-analysis.md) — **legacy per_query**. Переснять после persistent.

---

## Прагматичный порядок на кластере

1. `PROFILE=h2 ./scripts/hive/diagnose-session-tax.sh`
2. `SUITE=audei ./scripts/hive/run-hive-factor.sh h2` (+ опц. `SESSION_MODE=per_query` A/B)
3. `PROFILE=h2 ./scripts/hive/explain-audei.sh`
4. `SUITE=audei ./scripts/hive/run-hive-factor.sh h2f` (отдельный fetch path)
5. `llap-preflight` → `h3` / `h4`
6. Go/No-Go: если median interactive всё ещё &gt; 3 s при live LLAP — **Hive не target interactive**; продукт на Spark S1 + `best_orc`

---

## Ожидаемый итог

| Условие | Реалистичный latency |
|---|---|
| Legacy per_query Beeline | ~15–23 s (исторические H0–H4) |
| Persistent + Time taken, тот же ORC | часто единицы секунд; шанс &lt; 3 с на EQ |
| Persistent + live LLAP + prune + warm | шанс стабильно &lt; 3 с |
| Spark S1 + `best_orc` на L | уже **PASS** interactive SLA |
