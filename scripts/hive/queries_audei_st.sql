-- AUDEI interactive + ST archive query templates for Hive Beeline.
-- Placeholders: ${Y} ${M} ${D} ${EPK_ID} ${NAME} ${CHANNEL} ${STATUS}
--               ${TS_1D_START} ${TS_1D_END} ${TS_14D_START} ${TS_14D_END}
--               ${TS_31D_START} ${TS_31D_END} ${LIKE_TOKEN} ${RLIKE}
--               ${PART_1D} ${PART_14D} ${PART_31D} — optional " AND (event_year=… OR …)"
-- Canonical executable SQL is built in run-hive-factor.sh (partition predicates + projection).

-- === audei suite ===

-- epk_eq_1d
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_1D_START}' AND event_ts < TIMESTAMP '${TS_1D_END}'
  AND epk_id = '${EPK_ID}'
  ${PART_1D};

-- epk_eq_14d
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_14D_START}' AND event_ts < TIMESTAMP '${TS_14D_END}'
  AND epk_id = '${EPK_ID}'
  ${PART_14D};

-- epk_page (no payload_json)
SELECT epk_id, event_id, event_ts, name, channel_type, state, module
FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_14D_START}' AND event_ts < TIMESTAMP '${TS_14D_END}'
  AND epk_id = '${EPK_ID}'
  ${PART_14D}
ORDER BY event_ts
LIMIT 1000;

-- eq_filters
SELECT count(*) FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
  AND name = '${NAME}' AND channel_type = '${CHANNEL}' AND state = '${STATUS}';

-- order_by_epk_day (no payload_json)
SELECT epk_id, event_id, event_ts, name, channel_type, state, module
FROM events_ext
WHERE event_year = ${Y} AND event_month = ${M} AND event_day = ${D}
ORDER BY epk_id;

-- === st suite ===

-- no_filter
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  ${PART_31D};

-- like_single
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json LIKE '%${LIKE_TOKEN}%'
  ${PART_31D};

-- like_multi (5 AND LIKE)
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json LIKE '%${LIKE_TOKEN}%'
  AND payload_json LIKE '%AUDEI%'
  AND payload_json LIKE '%sms%'
  AND payload_json LIKE '%session%'
  AND payload_json LIKE '%pad%'
  ${PART_31D};

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
  AND payload_json LIKE '%event%'
  ${PART_31D};

-- eq
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND epk_id = '${EPK_ID}'
  ${PART_31D};

-- in_list
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND epk_id IN ('${EPK_ID}','${EPK_ID_B}','${EPK_ID_C}','${EPK_ID_D}')
  ${PART_31D};

-- rlike
SELECT count(*) FROM events_ext
WHERE event_ts >= TIMESTAMP '${TS_31D_START}' AND event_ts < TIMESTAMP '${TS_31D_END}'
  AND payload_json RLIKE '${RLIKE}'
  ${PART_31D};
