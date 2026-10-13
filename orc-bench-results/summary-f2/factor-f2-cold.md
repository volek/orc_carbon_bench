# factor-f2-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | f2 | f2 | spark | cold | module,name | spark32-orc | 5 | 939.00 | 885.00 | 1057.00 | 1057.00 | 0.00 | 566884791.00 | 0.03 | 100.0% |
| epk_eq_1d | f2 | f2 | spark | cold | module,name | spark32-orc | 5 | 1346.00 | 1287.00 | 1566.00 | 1566.00 | 0.00 | 566884791.00 | 0.03 | 100.0% |
| epk_page | f2 | f2 | spark | cold | module,name | spark32-orc | 5 | 884.40 | 825.00 | 1074.00 | 1074.00 | 0.00 | 566884791.00 | 0.03 | 100.0% |
| eq_filters | f2 | f2 | spark | cold | module,name | spark32-orc | 5 | 207.00 | 206.00 | 263.00 | 263.00 | 0.00 | 214455.00 | 0.00 | 100.0% |
| order_by_epk_day | f2 | f2 | spark | cold | module,name | spark32-orc | 5 | 158.80 | 141.00 | 189.00 | 189.00 | 0.00 | 132404.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | f2 | cold | 100.0% | 1057.00 | 1.78 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | f2 | cold | 100.0% | 1566.00 | 2.55 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | f2 | cold | 100.0% | 1074.00 | 1.68 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | f2 | cold | 100.0% | 263.00 | 1036.42 | 100.0% |
| order_by_epk_day | archive | - | 1_month | f2 | cold | 100.0% | 189.00 | 1287.80 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=f2 / dataset=f2 (1287.00 ms).

