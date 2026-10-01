# factor-e2-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | e2 | e2 | spark | cold | none | spark32-orc | 5 | 938.40 | 837.00 | 1198.00 | 1198.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| epk_eq_1d | e2 | e2 | spark | cold | none | spark32-orc | 5 | 1196.80 | 1202.00 | 1215.00 | 1215.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| epk_page | e2 | e2 | spark | cold | none | spark32-orc | 5 | 855.40 | 835.00 | 946.00 | 946.00 | 0.00 | 460553404.00 | 0.02 | 100.0% |
| eq_filters | e2 | e2 | spark | cold | none | spark32-orc | 5 | 235.00 | 182.00 | 418.00 | 418.00 | 0.00 | 249847.00 | 0.00 | 100.0% |
| order_by_epk_day | e2 | e2 | spark | cold | none | spark32-orc | 5 | 161.20 | 141.00 | 210.00 | 210.00 | 0.00 | 132320.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | e2 | cold | 100.0% | 1198.00 | 2.19 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | e2 | cold | 100.0% | 1215.00 | 2.79 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | e2 | cold | 100.0% | 946.00 | 1.99 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | e2 | cold | 100.0% | 418.00 | 1009.94 | 100.0% |
| order_by_epk_day | archive | - | 1_month | e2 | cold | 100.0% | 210.00 | 1308.10 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=e2 / dataset=e2 (1202.00 ms).

