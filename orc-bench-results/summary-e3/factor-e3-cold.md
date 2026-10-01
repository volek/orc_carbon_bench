# factor-e3-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | e3 | e3 | spark | cold | none | spark32-orc | 5 | 1075.60 | 1023.00 | 1421.00 | 1421.00 | 0.00 | 512902564.00 | 0.02 | 100.0% |
| epk_eq_1d | e3 | e3 | spark | cold | none | spark32-orc | 5 | 1081.60 | 1105.00 | 1259.00 | 1259.00 | 0.00 | 512902564.00 | 0.02 | 100.0% |
| epk_page | e3 | e3 | spark | cold | none | spark32-orc | 5 | 787.60 | 795.00 | 837.00 | 837.00 | 0.00 | 512902564.00 | 0.02 | 100.0% |
| eq_filters | e3 | e3 | spark | cold | none | spark32-orc | 5 | 214.40 | 185.00 | 382.00 | 382.00 | 0.00 | 201507.00 | 0.00 | 100.0% |
| order_by_epk_day | e3 | e3 | spark | cold | none | spark32-orc | 5 | 151.00 | 147.00 | 173.00 | 173.00 | 0.00 | 132310.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | e3 | cold | 100.0% | 1421.00 | 2.25 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | e3 | cold | 100.0% | 1259.00 | 2.26 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | e3 | cold | 100.0% | 837.00 | 1.65 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | e3 | cold | 100.0% | 382.00 | 1142.44 | 100.0% |
| order_by_epk_day | archive | - | 1_month | e3 | cold | 100.0% | 173.00 | 1225.42 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=e3 / dataset=e3 (1105.00 ms).

