# factor-b0-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | b0 | b0 | spark | cold | none | spark32-orc | 5 | 990.00 | 908.00 | 1201.00 | 1201.00 | 0.00 | 566962761.00 | 0.03 | 100.0% |
| epk_eq_1d | b0 | b0 | spark | cold | none | spark32-orc | 5 | 1168.40 | 1219.00 | 1290.00 | 1290.00 | 0.00 | 566962761.00 | 0.03 | 100.0% |
| epk_page | b0 | b0 | spark | cold | none | spark32-orc | 5 | 837.60 | 826.00 | 1060.00 | 1060.00 | 0.00 | 566962761.00 | 0.03 | 100.0% |
| eq_filters | b0 | b0 | spark | cold | none | spark32-orc | 5 | 195.40 | 176.00 | 286.00 | 286.00 | 0.00 | 212568.00 | 0.00 | 100.0% |
| order_by_epk_day | b0 | b0 | spark | cold | none | spark32-orc | 5 | 152.00 | 144.00 | 196.00 | 196.00 | 0.00 | 132310.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | b0 | cold | 100.0% | 1201.00 | 1.87 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | b0 | cold | 100.0% | 1290.00 | 2.21 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | b0 | cold | 100.0% | 1060.00 | 1.59 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | b0 | cold | 100.0% | 286.00 | 987.02 | 100.0% |
| order_by_epk_day | archive | - | 1_month | b0 | cold | 100.0% | 196.00 | 1233.53 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=b0 / dataset=b0 (1219.00 ms).

