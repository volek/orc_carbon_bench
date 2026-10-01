# factor-f1-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | f1 | f1 | spark | cold | epk_id,event_id | spark32-orc | 5 | 697.00 | 695.00 | 790.00 | 790.00 | 0.00 | 96455189.00 | 0.00 | 100.0% |
| epk_eq_1d | f1 | f1 | spark | cold | epk_id,event_id | spark32-orc | 5 | 1031.60 | 1077.00 | 1138.00 | 1138.00 | 0.00 | 96455189.00 | 0.00 | 100.0% |
| epk_page | f1 | f1 | spark | cold | epk_id,event_id | spark32-orc | 5 | 761.80 | 828.00 | 951.00 | 951.00 | 0.00 | 96455189.00 | 0.00 | 100.0% |
| eq_filters | f1 | f1 | spark | cold | epk_id,event_id | spark32-orc | 5 | 273.60 | 210.00 | 576.00 | 576.00 | 0.00 | 212610.00 | 0.00 | 100.0% |
| order_by_epk_day | f1 | f1 | spark | cold | epk_id,event_id | spark32-orc | 5 | 176.80 | 169.00 | 223.00 | 223.00 | 0.00 | 132400.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | f1 | cold | 100.0% | 790.00 | 7.76 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | f1 | cold | 100.0% | 1138.00 | 11.48 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | f1 | cold | 100.0% | 951.00 | 8.48 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | f1 | cold | 100.0% | 576.00 | 1381.76 | 100.0% |
| order_by_epk_day | archive | - | 1_month | f1 | cold | 100.0% | 223.00 | 1433.82 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=f1 / dataset=f1 (1077.00 ms).

