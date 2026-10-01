# factor-s1-cold

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | h0 | best_orc | hive_tez | cold | none | hive-tez-llap | 5 | 22161.60 | 22113.00 | 23126.00 | 23126.00 | 0.00 | 0.00 | 0.00 | 0.0% |
| epk_eq_14d | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 941.60 | 969.00 | 994.00 | 994.00 | 0.00 | 140225117.00 | 0.00 | 100.0% |
| epk_eq_1d | h0 | best_orc | hive_tez | cold | none | hive-tez-llap | 5 | 23872.00 | 22215.00 | 30527.00 | 30527.00 | 0.00 | 0.00 | 0.00 | 0.0% |
| epk_eq_1d | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 938.60 | 907.00 | 1233.00 | 1233.00 | 0.00 | 140225117.00 | 0.00 | 100.0% |
| epk_page | h0 | best_orc | hive_tez | cold | none | hive-tez-llap | 5 | 22360.20 | 22167.00 | 23963.00 | 23963.00 | 0.00 | 0.00 | 0.00 | 0.0% |
| epk_page | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 679.80 | 603.00 | 939.00 | 939.00 | 0.00 | 140225117.00 | 0.00 | 100.0% |
| eq_filters | h0 | best_orc | hive_tez | cold | none | hive-tez-llap | 5 | 16443.80 | 16386.00 | 16972.00 | 16972.00 | 0.00 | 0.00 | 0.00 | 0.0% |
| eq_filters | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 226.20 | 186.00 | 431.00 | 431.00 | 0.00 | 1143016.00 | 0.00 | 100.0% |
| order_by_epk_day | h0 | best_orc | hive_tez | cold | none | hive-tez-llap | 5 | 15744.40 | 15862.00 | 16158.00 | 16158.00 | 0.00 | 0.00 | 0.00 | 0.0% |
| order_by_epk_day | s1 | best_orc | spark | cold | none | spark32-orc | 5 | 200.00 | 143.00 | 419.00 | 419.00 | 0.00 | 591850.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | - | - | - | best_orc | cold | 0.0% | 23126.00 | - | - |
| epk_eq_14d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 994.00 | 7.21 | 100.0% |
| epk_eq_1d | - | - | - | best_orc | cold | 0.0% | 30527.00 | - | - |
| epk_eq_1d | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 1233.00 | 7.19 | 100.0% |
| epk_page | - | - | - | best_orc | cold | 0.0% | 23963.00 | - | - |
| epk_page | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 939.00 | 5.21 | 100.0% |
| eq_filters | - | - | - | best_orc | cold | 0.0% | 16972.00 | - | - |
| eq_filters | interactive | 7_EQ | 1_month | best_orc | cold | 100.0% | 431.00 | 212.49 | 0.0% |
| order_by_epk_day | - | - | - | best_orc | cold | 0.0% | 16158.00 | - | - |
| order_by_epk_day | archive | - | 1_month | best_orc | cold | 100.0% | 419.00 | 362.84 | 0.0% |

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
- Самый медленный сценарий по p50: `epk_eq_1d` / layout=best_orc / dataset=h0 (22215.00 ms).

