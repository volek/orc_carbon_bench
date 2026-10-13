# factor-f0-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | f0 | f0 | spark | cold | none | spark32-orc | 5 | 859.40 | 902.00 | 1092.00 | 1092.00 | 0.00 | 566863757.00 | 0.03 | 100.0% |
| epk_eq_1d | f0 | f0 | spark | cold | none | spark32-orc | 5 | 1014.60 | 1030.00 | 1178.00 | 1178.00 | 0.00 | 566863757.00 | 0.03 | 100.0% |
| epk_page | f0 | f0 | spark | cold | none | spark32-orc | 5 | 821.40 | 835.00 | 895.00 | 895.00 | 0.00 | 566863757.00 | 0.03 | 100.0% |
| eq_filters | f0 | f0 | spark | cold | none | spark32-orc | 5 | 198.80 | 155.00 | 374.00 | 374.00 | 0.00 | 212549.00 | 0.00 | 100.0% |
| order_by_epk_day | f0 | f0 | spark | cold | none | spark32-orc | 5 | 145.40 | 135.00 | 197.00 | 197.00 | 0.00 | 132308.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | f0 | cold | 100.0% | 1092.00 | 1.63 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | f0 | cold | 100.0% | 1178.00 | 1.92 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | f0 | cold | 100.0% | 895.00 | 1.56 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | f0 | cold | 100.0% | 374.00 | 1004.29 | 100.0% |
| order_by_epk_day | archive | - | 1_month | f0 | cold | 100.0% | 197.00 | 1179.99 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=f0 / dataset=f0 (1030.00 ms).

