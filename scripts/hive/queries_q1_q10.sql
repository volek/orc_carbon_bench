-- Q1–Q10 Hive SQL suite (mapped to existing columns). Placeholders: ${Y} ${M} ${D} ${EVENT_ID} ${USER_ID}
-- ${PRODUCT_ID} ${CAMPAIGN_ID} ${STATUS} ${COUNTRY} ${TS_START} ${TS_END}

-- Q1 partition_prune
SELECT count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q2 filter_high_cardinality
SELECT event_year, event_id
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND (event_id = '${EVENT_ID}' OR user_id = ${USER_ID});

-- Q3 filter_medium_cardinality
SELECT event_year, product_id
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND (product_id = ${PRODUCT_ID} OR campaign_id = ${CAMPAIGN_ID});

-- Q4 filter_low_cardinality
SELECT count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND country_code = '${COUNTRY}' AND status = '${STATUS}';

-- Q5 filter_in
SELECT *
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND event_id IN ('${EVENT_ID}', '${EVENT_ID}-missing-a', '${EVENT_ID}-missing-b', '${EVENT_ID}-missing-c');

-- Q6 filter_timestamp_range
SELECT count(*) AS cnt
FROM events_ext
WHERE `timestamp` >= '${TS_START}' AND `timestamp` < '${TS_END}';

-- Q7 projection (column pruning)
SELECT event_id, user_id, `timestamp`, amount, log_format
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q7b full_scan same partition
SELECT *
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q8 group_by
SELECT country_code, device_type, status, count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
GROUP BY country_code, device_type, status;

-- Q9 group_by_heavy
SELECT product_id, campaign_id, country_code, count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M}
GROUP BY product_id, campaign_id, country_code;

-- Q10 join_dictionary
SELECT e.product_id, count(*) AS cnt
FROM events_ext e
JOIN dictionary_ext d ON e.product_id = d.product_id
WHERE e.event_year = ${Y} AND e.event_month = ${M} AND e.event_day = ${D}
  AND d.product_type = 'featured'
GROUP BY e.product_id;
