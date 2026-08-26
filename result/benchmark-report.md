# benchmark-report

Сводный отчёт бенчмарка ORC на кластерном Spark 3.2.

## Benchmark Summary

| scenario | spark_runtime | runs | avg_ms | p50_ms | p95_ms | avg_selectivity |
|---|---|---:|---:|---:|---:|---:|
| filter_combined | spark32-orc | 1 | 186063.00 | 186063.00 | 186063.00 | 0.25 |
| filter_high_cardinality | spark32-orc | 1 | 180080.00 | 180080.00 | 180080.00 | 0.00 |
| filter_log_format | spark32-orc | 1 | 185115.00 | 185115.00 | 185115.00 | 0.25 |
| filter_low_cardinality | spark32-orc | 1 | 186255.00 | 186255.00 | 186255.00 | 0.01 |
| filter_medium_cardinality | spark32-orc | 1 | 189618.00 | 189618.00 | 189618.00 | 0.00 |
| filter_timestamp_range | spark32-orc | 1 | 182244.00 | 182244.00 | 182244.00 | 1.00 |
| full_scan | spark32-orc | 1 | 190419.00 | 190419.00 | 190419.00 | 1.00 |
| group_by | spark32-orc | 1 | 194048.00 | 194048.00 | 194048.00 | 0.00 |
| projection | spark32-orc | 1 | 187791.00 | 187791.00 | 187791.00 | 1.00 |
| text_search | spark32-orc | 1 | 190956.00 | 190956.00 | 190956.00 | 0.25 |

## Validation

_Нет данных валидации_

## Recommendations

- Самый медленный сценарий по p50: `group_by` (194048.00 ms) — кандидат на оптимизацию фильтров/проекций ORC.

