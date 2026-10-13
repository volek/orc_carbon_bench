# bloom-ab-report

Сводный отчёт бенчмарка ORC на кластерном Spark 3.2.

## Benchmark Summary

| scenario | dataset | bloom_columns | spark_runtime | runs | avg_ms | p50_ms | p95_ms | avg_selectivity | avg_bytes_read | avg_records_read |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| filter_combined | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 1065.20 | 1054.00 | 1116.00 | 0.02 | 140770639.00 | 53687091.00 |
| filter_combined | nobloom | none | spark32-orc | 5 | 1183.00 | 1214.00 | 1321.00 | 0.02 | 140535599.00 | 53687091.00 |
| filter_high_cardinality | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 4346.40 | 4350.00 | 4568.00 | 0.00 | 3093701131.00 | 19810258.00 |
| filter_high_cardinality | nobloom | none | spark32-orc | 5 | 11770.00 | 11705.00 | 13177.00 | 0.00 | 14691806296.00 | 214748364.00 |
| filter_log_format | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 547.60 | 554.00 | 575.00 | 0.25 | 24524034.00 | 53687091.00 |
| filter_log_format | nobloom | none | spark32-orc | 5 | 549.80 | 557.00 | 600.00 | 0.25 | 24475426.00 | 53687091.00 |
| filter_low_cardinality | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 3417.40 | 3311.00 | 4037.00 | 0.01 | 164177696.00 | 214748364.00 |
| filter_low_cardinality | nobloom | none | spark32-orc | 5 | 3349.60 | 3230.00 | 3606.00 | 0.01 | 163942730.00 | 214748364.00 |
| filter_medium_cardinality | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 4228.60 | 4202.00 | 4633.00 | 0.00 | 1780268665.00 | 214748364.00 |
| filter_medium_cardinality | nobloom | none | spark32-orc | 5 | 4442.60 | 3558.00 | 6875.00 | 0.00 | 1437129553.00 | 214748364.00 |
| filter_timestamp_range | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 3470.60 | 3493.00 | 3639.00 | 0.08 | 562152454.00 | 214748364.00 |
| filter_timestamp_range | nobloom | none | spark32-orc | 5 | 3373.80 | 3285.00 | 3544.00 | 0.08 | 561774644.00 | 214748364.00 |
| full_scan | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 1908.80 | 1846.00 | 2245.00 | 1.00 | 98087764.00 | 214748364.00 |
| full_scan | nobloom | none | spark32-orc | 5 | 1951.60 | 2019.00 | 2246.00 | 1.00 | 97893616.00 | 214748364.00 |
| group_by | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 4456.20 | 4430.00 | 4676.00 | 0.00 | 125884531.00 | 214748364.00 |
| group_by | nobloom | none | spark32-orc | 5 | 4299.60 | 4303.00 | 4451.00 | 0.00 | 125605925.00 | 214748364.00 |
| projection | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 1639.80 | 1600.00 | 1762.00 | 1.00 | 98087764.00 | 214748364.00 |
| projection | nobloom | none | spark32-orc | 5 | 1748.60 | 1693.00 | 1980.00 | 1.00 | 97893616.00 | 214748364.00 |
| text_search | bloom | event_id,user_id,product_id,campaign_id | spark32-orc | 5 | 20666.60 | 20649.00 | 21032.00 | 0.25 | 26273356195.00 | 214748364.00 |
| text_search | nobloom | none | spark32-orc | 5 | 18975.00 | 18995.00 | 19158.00 | 0.25 | 26272936879.00 | 214748364.00 |

## Bloom filter comparison

| scenario | nobloom bytes_read | bloom bytes_read | Δ bytes | nobloom p50_ms | bloom p50_ms |
|---|---:|---:|---:|---:|---:|
| filter_high_cardinality | 14691806296.00 | 3093701131.00 | -78.9% | 11705.00 | 4350.00 |
| filter_medium_cardinality | 1437129553.00 | 1780268665.00 | +23.9% | 3558.00 | 4202.00 |
| filter_combined | 140535599.00 | 140770639.00 | +0.2% | 1214.00 | 1054.00 |

## Validation

| check | passed |
|---|---|
| log_format_distribution | PASS |
| log_format_distribution | PASS |
| log_message_structure | PASS |
| log_message_structure | PASS |
| low_cardinality_bounds | PASS |
| low_cardinality_bounds | PASS |
| orc_bloom_filters | PASS |
| orc_bloom_filters | PASS |
| row_count | PASS |
| row_count | PASS |
| timestamp_range | PASS |
| timestamp_range | PASS |

## Recommendations

- Bloom A/B на `filter_high_cardinality`: avg_bytes_read bloom=3093701131 vs nobloom=14691806296 (экономия ~78.9%).
- Самый медленный сценарий по p50: `text_search` / dataset=bloom (20649.00 ms).

