# factor-d2-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | d2 | d2 | spark | cold | none | spark32-orc | 5 | 3940.20 | 4144.00 | 4316.00 | 4316.00 | 0.00 | 988211446.00 | 0.04 | 0.0% |
| epk_eq_1d | d2 | d2 | spark | cold | none | spark32-orc | 5 | 5400.00 | 5396.00 | 6126.00 | 6126.00 | 0.00 | 988211446.00 | 0.04 | 0.0% |
| epk_page | d2 | d2 | spark | cold | none | spark32-orc | 5 | 3870.00 | 3733.00 | 4624.00 | 4624.00 | 0.00 | 988211446.00 | 0.04 | 0.0% |
| eq_filters | d2 | d2 | spark | cold | none | spark32-orc | 5 | 261.60 | 245.00 | 328.00 | 328.00 | 0.00 | 1218492.00 | 0.00 | 100.0% |
| order_by_epk_day | d2 | d2 | spark | cold | none | spark32-orc | 5 | 234.20 | 239.00 | 245.00 | 245.00 | 0.00 | 800422.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | d2 | cold | 0.0% | 4316.00 | 4.28 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | d2 | cold | 0.0% | 6126.00 | 5.87 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | d2 | cold | 0.0% | 4624.00 | 4.20 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | d2 | cold | 100.0% | 328.00 | 230.52 | 100.0% |
| order_by_epk_day | archive | - | 1_month | d2 | cold | 100.0% | 245.00 | 314.17 | 0.0% |

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

- SLA success ниже 98% (мин=0.0%). Смотрите p95/p99 и cold vs warm отдельно.
- Самый медленный сценарий по p50: `epk_eq_1d` / layout=d2 / dataset=d2 (5396.00 ms).

