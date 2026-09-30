-- Q1–Q10 Hive SQL mapped to AUDEI audit columns (placeholders filled by runner).

-- Q1 partition_prune
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q2 filter_high_cardinality
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND (epk_id = '${EPK_ID}' OR event_id = '${EVENT_ID}');

-- Q3 filter_medium_cardinality
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND (module = '${MODULE}' OR name = '${NAME}');

-- Q4 filter_low_cardinality
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND channel_type = '${CHANNEL}' AND state = '${STATUS}';

-- Q5 filter_in
SELECT count(*) FROM events_ext
WHERE event_id IN (${EVENT_ID_IN}) OR epk_id IN (${EPK_ID_IN});

-- Q6 filter_timestamp_range
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_START}' AND event_ts < TIMESTAMP '${TS_END}';

-- Q7 projection
SELECT epk_id, event_id, event_ts, name, channel_type
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q7 full_scan (scoped day for Hive cost control)
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D};

-- Q8 group_by
SELECT name, channel_type, state, count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
GROUP BY name, channel_type, state;

-- Q9 group_by_heavy
SELECT module, name, channel_type, count(*) AS cnt
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M}
GROUP BY module, name, channel_type;

-- Q10 join_dictionary
SELECT e.name, count(*) AS cnt
FROM events_ext e
JOIN dictionary_ext d ON e.name = d.event_name
WHERE e.event_year = ${Y} AND e.event_month = ${M} AND e.event_day = ${D}
  AND d.event_family = 'featured'
GROUP BY e.name;
