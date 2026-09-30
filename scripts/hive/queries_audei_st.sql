-- AUDEI interactive + ST archive query templates for Hive Beeline.
-- Placeholders: ${Y} ${M} ${D} ${EPK_ID} ${NAME} ${CHANNEL} ${STATUS}
--               ${TS_1D_START} ${TS_1D_END} ${TS_14D_START} ${TS_14D_END}
--               ${TS_31D_START} ${TS_31D_END} ${LIKE_TOKEN} ${RLIKE}

-- === audei suite ===

-- epk_eq_1d
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_1D_START}' AND event_ts < TIMESTAMP '${TS_1D_END}'
  AND epk_id = '${EPK_ID}';

-- epk_eq_14d
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_14D_START}' AND event_ts < TIMESTAMP '${TS_14D_END}'
  AND epk_id = '${EPK_ID}';

-- epk_page
SELECT * FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_14D_START}' AND event_ts < TIMESTAMP '${TS_14D_END}'
  AND epk_id = '${EPK_ID}'
ORDER BY event_ts
LIMIT 1000;

-- eq_filters
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND name = '${NAME}' AND channel_type = '${CHANNEL}' AND state = '${STATUS}';

-- order_by_epk_day
SELECT * FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
ORDER BY epk_id;

-- === st suite ===

-- no_filter
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}';

-- like_single
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json LIKE '%${LIKE_TOKEN}%';

-- like_multi (5 AND LIKE)
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json LIKE '%${LIKE_TOKEN}%'
  AND payload_json LIKE '%AUDEI%'
  AND payload_json LIKE '%sms%'
  AND payload_json LIKE '%session%'
  AND payload_json LIKE '%pad%';

-- like_fulltext (≥10 AND LIKE)
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json LIKE '%${LIKE_TOKEN}%'
  AND payload_json LIKE '%AUDEI%'
  AND payload_json LIKE '%sms%'
  AND payload_json LIKE '%session%'
  AND payload_json LIKE '%pad%'
  AND payload_json LIKE '%device%'
  AND payload_json LIKE '%confirm%'
  AND payload_json LIKE '%metamodel%'
  AND payload_json LIKE '%param%'
  AND payload_json LIKE '%event%';

-- eq
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND epk_id = '${EPK_ID}';

-- in_list
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND epk_id IN ('${EPK_ID}','${EPK_ID_B}','${EPK_ID_C}','${EPK_ID_D}');

-- rlike
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json RLIKE '${RLIKE}';
