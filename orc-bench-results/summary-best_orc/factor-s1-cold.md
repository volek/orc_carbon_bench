# factor-s1-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 1130.80 | 1124.00 | 1275.00 | 1275.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| epk_eq_1d | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 1290.00 | 1297.00 | 1528.00 | 1528.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| epk_page | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 678.40 | 686.00 | 858.00 | 858.00 | 0.00 | 60606057.00 | 0.00 | 100.0% |
| eq_filters | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 187.00 | 146.00 | 299.00 | 299.00 | 0.00 | 249911.00 | 0.00 | 100.0% |
| order_by_epk_day | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 150.00 | 142.00 | 174.00 | 174.00 | 0.00 | 132384.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1275.00 | 20.03 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1528.00 | 22.85 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 858.00 | 12.02 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 299.00 | 803.44 | 100.0% |
| order_by_epk_day | archive | - | 1_month | best_orc | cold | 100.0% | 174.00 | 1216.62 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=best_orc / dataset=s1 (1297.00 ms).

