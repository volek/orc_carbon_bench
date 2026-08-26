# Выводы по результатам бенчмарка ORC (smoke)

Документ фиксирует интерпретацию прогона, отражённого в `result/benchmark-report.md`  
(сырые агрегаты: `result/results.csv`, `result/results.json`).

| Поле | Значение |
|---|---|
| Runtime | `spark32-orc` |
| Spark | `3.2.1.3.5.7.0-1-SNAPSHOT` |
| Format | ORC |
| Измерений на сценарий (`runs`) | **1** |
| Validation в отчёте | **нет данных** |
| Метрики I/O (`bytes_read`) | **не собирались** (прогон до доработки) |

---

## Вердикт

Прогон зафиксировал **узкий ORC-baseline** (~3 мин wall time на сценарий), а не ранжирование оптимизаций и не сравнение форматов.

- Среднее время по 10 сценариям: **~187 с**
- Разброс max−min: **~14 с (~7.8%)**
- При `runs=1` значения avg / p50 / p95 совпадают — дисперсии нет
- Выводы о «победителях» сценариев **статистически слабые**

Этот результат достаточен как **smoke / baseline**, недостаточен как **адекватная картина бенчмарка** (см. [cluster_manual_runbook.md](cluster_manual_runbook.md) §4).

---

## Сводка по сценариям

Времена упорядочены по возрастанию; отклонение — относительно `full_scan` (190419 ms).

| Сценарий | Время | vs full_scan | Selectivity | Комментарий |
|---|---:|---:|---:|---|
| `filter_high_cardinality` | 180.1 с | −5.4% | ~9.3e−8 | point lookup, почти пустой результат |
| `filter_timestamp_range` | 182.2 с | −4.3% | **1.00** | фильтр не отсекает строки |
| `filter_log_format` | 185.1 с | −2.8% | 0.25 | |
| `filter_combined` | 186.1 с | −2.3% | 0.25 | |
| `filter_low_cardinality` | 186.3 с | −2.2% | ~0.01 | |
| `projection` | 187.8 с | −1.4% | 1.00 | column pruning почти незаметен |
| `filter_medium_cardinality` | 189.6 с | −0.4% | ~2.3e−4 | |
| `full_scan` | 190.4 с | baseline | 1.00 | |
| `text_search` | 191.0 с | +0.3% | 0.25 | substring, чуть дороже scan |
| `group_by` | 194.0 с | +1.9% | ~4.7e−6 | самый медленный; overhead агрегации |

---

## Ключевые наблюдения

### 1. Selectivity почти не влияет на время

`filter_high_cardinality` возвращает почти 0 строк, но быстрее `full_scan` лишь на **~5%**.  
Либо ORC pruning/pushdown слабо проявляется на этом прогоне, либо доминирует фиксированная стоимость job / чтения почти всех stripe.

### 2. `filter_timestamp_range` — ложный selective-case

Selectivity = **1.0**: фильтр по timestamp совпал со всем generate-окном данных.  
Сценарий **не проверял** range-pruning. (Исправлено в коде: `--benchmark-timestamp-window-days`, см. runbook.)

### 3. Projection даёт мало

Выбор подмножества колонок: **−1.4%** к full scan. Выгода column pruning на wall time в этом прогоне минимальна.

### 4. `group_by` — не «плохой фильтр ORC»

Самый медленный (+1.9% / +3.6 с к full scan) из‑за агрегации.  
Авто-рекомендация отчёта («оптимизация фильтров/проекций») здесь **слабо обоснована**.

### 5. Validation отсутствует

Секция Validation пуста. Вероятная причина на том пайплайне: overwrite метрик benchmark в `reports/raw/` затирал `raw/validation/`.  
(Исправлено: метрики пишутся в `raw/benchmark/`.)

### 6. Нет Carbon A/B и нет I/O-метрик

В отчёте только ORC / `spark32-orc`. Carbon в проекте out of scope.  
`bytes_read` / `records_read` в этом прогоне ещё не собирались — pruning по I/O не оценить.

---

## Ограничения прогона

1. **`runs=1`** — нет устойчивых p50/p95  
2. Малый smoke-объём (типично `--target-size-tb=0.01`) — разброс сценариев тонет в overhead  
3. Нет Validation в summary  
4. Нет сравнения с другим форматом/runtime  
5. Cache base DF в старом коде мог маскировать predicate pushdown (исправлено в текущей ветке)

---

## Практические следствия

**Что можно считать установленным**

- Baseline wall time ORC на данном кластерном Spark 3.2 и датасете: порядок **180–194 с** на сценарий.
- Пайплайн бенчмарка отрабатывает end-to-end (с оговорками по Validation и селективности timestamp).

**Чего делать нельзя на этих цифрах**

- Ранжировать ORC-оптимизации и паттерны запросов  
- Утверждать эффективность stripe pruning  
- Сравнивать с Carbon / другим runtime  

**Что запускать дальше (адекватная картина)**

См. [cluster_manual_runbook.md](cluster_manual_runbook.md) §4:

| Параметр | Минимум для выводов |
|---|---|
| `--target-size-tb` | **≥ 0.1** (не 0.01) |
| `--benchmark-repeat-runs` | **≥ 3** (лучше 5) |
| `--benchmark-timestamp-window-days` | 30 |
| `--clear-cache-between-runs` | `true` |
| Validation | секция заполнена, checks PASS |
| `avg_bytes_read` | selective-фильтры заметно ниже `full_scan` |

---

## Источники

- `result/benchmark-report.md` — сводный Markdown-отчёт  
- `result/results.csv/` / `result/results.json/` — агрегаты  
- [cluster_manual_runbook.md](cluster_manual_runbook.md) — smoke vs Pilot, ресурсы YARN, критерии адекватности  
