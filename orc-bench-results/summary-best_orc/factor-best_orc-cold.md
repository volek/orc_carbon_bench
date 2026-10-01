# factor-best_orc-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | o1_cbo_on | best_orc | spark | cold | epk_id | spark32-orc | 5 | 906.20 | 839.00 | 1194.00 | 1194.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| epk_eq_1d | o1_cbo_on | best_orc | spark | cold | epk_id | spark32-orc | 5 | 1033.60 | 1040.00 | 1332.00 | 1332.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| epk_page | o1_cbo_on | best_orc | spark | cold | epk_id | spark32-orc | 5 | 772.40 | 801.00 | 889.00 | 889.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| eq_filters | o1_cbo_on | best_orc | spark | cold | epk_id | spark32-orc | 5 | 221.20 | 163.00 | 445.00 | 445.00 | 0.00 | 249911.00 | 0.00 | 100.0% |
| order_by_epk_day | o1_cbo_on | best_orc | spark | cold | epk_id | spark32-orc | 5 | 159.20 | 144.00 | 210.00 | 210.00 | 0.00 | 132384.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1194.00 | 16.05 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1332.00 | 18.31 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 889.00 | 13.68 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 445.00 | 950.39 | 100.0% |
| order_by_epk_day | archive | - | 1_month | best_orc | cold | 100.0% | 210.00 | 1291.24 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=best_orc / dataset=o1_cbo_on (1040.00 ms).

