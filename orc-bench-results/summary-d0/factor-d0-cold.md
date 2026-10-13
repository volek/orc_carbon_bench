# factor-d0-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | d0 | d0 | spark | cold | none | spark32-orc | 5 | 691.00 | 686.00 | 852.00 | 852.00 | 0.00 | 547288123.00 | 0.02 | 100.0% |
| epk_eq_1d | d0 | d0 | spark | cold | none | spark32-orc | 5 | 996.80 | 950.00 | 1176.00 | 1176.00 | 0.00 | 547288123.00 | 0.02 | 100.0% |
| epk_page | d0 | d0 | spark | cold | none | spark32-orc | 5 | 609.20 | 583.00 | 772.00 | 772.00 | 0.00 | 547288123.00 | 0.02 | 100.0% |
| eq_filters | d0 | d0 | spark | cold | none | spark32-orc | 5 | 544.00 | 496.00 | 769.00 | 769.00 | 0.00 | 23586551.00 | 0.00 | 100.0% |
| order_by_epk_day | d0 | d0 | spark | cold | none | spark32-orc | 5 | 401.60 | 407.00 | 447.00 | 447.00 | 0.00 | 18530018.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | d0 | cold | 100.0% | 852.00 | 1.36 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | d0 | cold | 100.0% | 1176.00 | 1.96 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | d0 | cold | 100.0% | 772.00 | 1.20 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | d0 | cold | 100.0% | 769.00 | 24.76 | 100.0% |
| order_by_epk_day | archive | - | 1_month | d0 | cold | 100.0% | 447.00 | 23.27 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=d0 / dataset=d0 (950.00 ms).

