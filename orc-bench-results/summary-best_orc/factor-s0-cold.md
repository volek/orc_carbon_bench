# factor-s0-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | s0 | best_orc | spark | cold | none | spark32-orc | 5 | 1698.40 | 1048.00 | 4184.00 | 4184.00 | 0.00 | 574214780.00 | 0.03 | 80.0% |
| epk_eq_1d | s0 | best_orc | spark | cold | none | spark32-orc | 5 | 1133.20 | 1160.00 | 1203.00 | 1203.00 | 0.00 | 574214780.00 | 0.03 | 100.0% |
| epk_page | s0 | best_orc | spark | cold | none | spark32-orc | 5 | 962.80 | 908.00 | 1085.00 | 1085.00 | 0.00 | 574214780.00 | 0.03 | 100.0% |
| eq_filters | s0 | best_orc | spark | cold | none | spark32-orc | 5 | 266.20 | 226.00 | 413.00 | 413.00 | 0.00 | 183969.00 | 0.00 | 100.0% |
| order_by_epk_day | s0 | best_orc | spark | cold | none | spark32-orc | 5 | 157.40 | 159.00 | 169.00 | 169.00 | 0.00 | 132384.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | best_orc | cold | 80.0% | 4184.00 | 3.18 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1203.00 | 2.12 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1085.00 | 1.80 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 413.00 | 1553.69 | 100.0% |
| order_by_epk_day | archive | - | 1_month | best_orc | cold | 100.0% | 169.00 | 1276.64 | 0.0% |

_`interactive` = AUDEI API path (default 3000 ms); `archive` = ST scan (default 120000 ms). `rows_cap` = доля прогонов с `rows_returned ≤ 2000`._

## Bloom filter comparison

_Нет A/B данных (нужны прогоны с `dataset_label=bloom` и `dataset_label=nobloom`)._

## Validation

| check | passed |
|---|---|
| log_format_distribution | PASS |
| log_message_structure | PASS |
| low_cardinality_bounds | PASS |
| orc_bloom_filters | PASS |
| row_count | PASS |
| timestamp_range | PASS |

## Recommendations

- SLA success ниже 98% (мин=80.0%). Смотрите p95/p99 и cold vs warm отдельно.
- Самый медленный сценарий по p50: `epk_eq_1d` / layout=best_orc / dataset=s0 (1160.00 ms).

