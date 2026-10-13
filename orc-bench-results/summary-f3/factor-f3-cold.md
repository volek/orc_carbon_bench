# factor-f3-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | f3 | f3 | spark | cold | epk_id,event_id,module,name,channel_type,state | spark32-orc | 5 | 957.20 | 965.00 | 1160.00 | 1160.00 | 0.00 | 95322590.00 | 0.00 | 100.0% |
| epk_eq_1d | f3 | f3 | spark | cold | epk_id,event_id,module,name,channel_type,state | spark32-orc | 5 | 1080.40 | 1086.00 | 1244.00 | 1244.00 | 0.00 | 95322590.00 | 0.00 | 100.0% |
| epk_page | f3 | f3 | spark | cold | epk_id,event_id,module,name,channel_type,state | spark32-orc | 5 | 690.20 | 661.00 | 862.00 | 862.00 | 0.00 | 95322590.00 | 0.00 | 100.0% |
| eq_filters | f3 | f3 | spark | cold | epk_id,event_id,module,name,channel_type,state | spark32-orc | 5 | 227.80 | 232.00 | 275.00 | 275.00 | 0.00 | 218919.00 | 0.00 | 100.0% |
| order_by_epk_day | f3 | f3 | spark | cold | epk_id,event_id,module,name,channel_type,state | spark32-orc | 5 | 210.20 | 197.00 | 330.00 | 330.00 | 0.00 | 132542.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | f3 | cold | 100.0% | 1160.00 | 10.78 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | f3 | cold | 100.0% | 1244.00 | 12.17 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | f3 | cold | 100.0% | 862.00 | 7.77 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | f3 | cold | 100.0% | 275.00 | 1117.30 | 100.0% |
| order_by_epk_day | archive | - | 1_month | f3 | cold | 100.0% | 330.00 | 1702.86 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=f3 / dataset=f3 (1086.00 ms).

