# sla-matrix-final

Сводный отчёт бенчмарка ORC (Spark 3.2 / Hive factor matrix).

## Benchmark Summary

| scenario | dataset | layout | engine | cache | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | p99_ms | avg_selectivity | avg_bytes_read | avg_scan_ratio | sla_success |
|---|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| epk_eq_14d | bloom | default | spark | cold | epk_id | spark32-orc | 3 | 1015.67 | 1011.00 | 1052.00 | 1052.00 | 0.00 | 54972440.00 | 0.00 | 100.0% |
| epk_eq_1d | bloom | default | spark | cold | epk_id | spark32-orc | 3 | 1417.33 | 1460.00 | 1684.00 | 1684.00 | 0.00 | 54972440.00 | 0.00 | 100.0% |
| epk_page | bloom | default | spark | cold | epk_id | spark32-orc | 3 | 721.33 | 740.00 | 849.00 | 849.00 | 0.00 | 54972440.00 | 0.00 | 100.0% |
| eq_filters | bloom | default | spark | cold | epk_id | spark32-orc | 3 | 367.00 | 438.00 | 441.00 | 441.00 | 0.00 | 175096.00 | 0.00 | 100.0% |
| order_by_epk_day | bloom | default | spark | cold | epk_id | spark32-orc | 3 | 178.67 | 184.00 | 193.00 | 193.00 | 0.00 | 99632.00 | 0.00 | 100.0% |

## SLA (interactive ≤ 3s / archive soft ceiling)

| scenario | sla_class | category | duration_group | layout | cache | sla_success | p95_ms | avg_sec/GB | rows_cap |
|---|---|---|---|---|---|---:|---:|---:|---:|
| epk_eq_14d | interactive | 7_EQ | 1_month | default | cold | 100.0% | 1052.00 | 19.84 | 100.0% |
| epk_eq_1d | interactive | 7_EQ | 1_month | default | cold | 100.0% | 1684.00 | 27.68 | 100.0% |
| epk_page | interactive | 7_EQ | 1_month | default | cold | 100.0% | 849.00 | 14.09 | 100.0% |
| eq_filters | interactive | 7_EQ | 1_month | default | cold | 100.0% | 441.00 | 2250.56 | 100.0% |
| order_by_epk_day | archive | - | 1_month | default | cold | 100.0% | 193.00 | 1925.50 | 0.0% |

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

- Самый медленный сценарий по p50: `epk_eq_1d` / layout=default / dataset=bloom (1460.00 ms).

