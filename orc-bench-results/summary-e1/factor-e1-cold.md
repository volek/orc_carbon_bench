# factor-e1-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | e1 | e1 | spark | cold | none | spark32-orc | 5 | 1065.40 | 1094.00 | 1148.00 | 1148.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| epk_eq_1d | e1 | e1 | spark | cold | none | spark32-orc | 5 | 1144.00 | 1154.00 | 1389.00 | 1389.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| epk_page | e1 | e1 | spark | cold | none | spark32-orc | 5 | 994.60 | 1050.00 | 1153.00 | 1153.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| eq_filters | e1 | e1 | spark | cold | none | spark32-orc | 5 | 221.80 | 202.00 | 322.00 | 322.00 | 0.00 | 249847.00 | 0.00 | 100.0% |
| order_by_epk_day | e1 | e1 | spark | cold | none | spark32-orc | 5 | 182.60 | 167.00 | 226.00 | 226.00 | 0.00 | 132320.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | e1 | cold | 100.0% | 1148.00 | 2.48 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | e1 | cold | 100.0% | 1389.00 | 2.67 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | e1 | cold | 100.0% | 1153.00 | 2.32 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | e1 | cold | 100.0% | 322.00 | 953.21 | 100.0% |
| order_by_epk_day | archive | - | 1_month | e1 | cold | 100.0% | 226.00 | 1481.75 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=e1 / dataset=e1 (1154.00 ms).

