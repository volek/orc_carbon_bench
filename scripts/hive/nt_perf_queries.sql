-- =====================================================================================================================
-- НТ: верификация механизмов производительности (расширение spark_queries.sql).
--
-- Каждый тест (T1-T18) = тот же load при разных настройках (A/B) или на разных датасетах.
-- Заключения делаются по метрикам, а не по данным: значения плейсхолдеров в результат не выводятся.
--
-- Метрики (Spark UI / лог раннера):
--   * длительность запроса (wall time)
--   * Executor CPU Time
--   * Shuffle Read/Write (bytes, records), количество shuffle-партаций
--   * Scan ORC: bytes read (input metrics), строк прочитано vs строк возвращено
--   * количество файлов/партиций в физическом плане (EXPLAIN / description в UI)
--   * EXPLAIN [COSTED]: форма плана (PushedFilters, BroadcastHashJoin/SortMergeJoin, DPP, AQE)
--
-- A/B-настройки задаются через --conf при запуске spark-shell (для каждого варианта свой запуск),
-- не внутри файла. Настройки указаны в каждом тесте.
--
-- Плейсхолдеры (sed, как в spark_queries.sql):
--   <INDEX_PATH>      - путь к каталогу индекса в HDFS
--   <PARTITION_KEY>   - медианная партиция (см. hdfs_stats.sh)
--   <RANGE_KEYS>      - 3-7 соседних партиций, {k1,k2,k3}
--   <RANGE_KEYS_WIDE> - 10-30 партиций, {k1,...,k30} (для T9/T16)
--   <FILE_ORC>        - полный путь одного ORC-файла в партиции (hdfs dfs -ls <...>/key=<PARTITION_KEY>)
--   <VAL_NAME>        - name типа события с долей ~1% (по результату 2.1 spark_queries.sql)
--   <VAL_NAME2>       - второй name с долей ~1-5% (2.1)
--   <VAL_MODULE>      - module (2.2)
--   <VAL_USERNODE>    - userNode (5.1)
--   <VAL_LOGIN>       - userLogin с числом событий ~p50 (5.2); значение в результат не выводится
--   <VAL_LOGINS>      - список 5-10 значений userLogin в виде ('a','b',...)
--   <T_FROM> <T_TO>       - широкий временной коридор ~1/3 партиции (по 6.1)
--   <T_NFROM> <T_NTO>     - узкий коридор ~1 час внутри партиции (по 6.1)
--   <T_MIN_FROM> <T_MIN_TO> - коридор ~1 минута (по 6.1)
--   <HEAVY_EPK>       - EPK_ID одного из топ-клиентов (4.4); значение в результат не выводится
--   <DPP_KEY_LIST>    - 2-3 ключа партиций из <RANGE_KEYS_WIDE> в виде ('k1','k2','k3')
--   <TABLE_PATH>      - каталог таблицы каталога Spark/Hive с тестовыми данными (T10, T17)
--
-- Подготовка и запуск - как для spark_queries.sql (sed > q.sql, затем run_spark_queries.scala).
-- Значения <VAL_LOGIN>/<HEAVY_EPK> использовать как литералы фильтров (как в проде), но НЕ выводить в результат.
-- =====================================================================================================================


SET spark.sql.sources.partitionColumnTypeInference.enabled=false;
SET spark.sql.ansi.enabled=false;


-- =====================================================================================================================
-- 0. Предпросмотрки (копия раздела 0 spark_queries.sql + предпросмотрки на несколько партиций)
-- =====================================================================================================================

CREATE OR REPLACE TEMPORARY VIEW part_raw USING orc
OPTIONS (path '<INDEX_PATH>/key=<PARTITION_KEY>', basePath '<INDEX_PATH>');

CREATE OR REPLACE TEMPORARY VIEW p AS
SELECT *,
       input_file_name() AS src_file,
       length(to_json(struct(*))) AS row_json_len,
       CASE WHEN array_position(paramName, 'EPK_ID') > 0
            THEN paramValue[CAST(array_position(paramName, 'EPK_ID') - 1 AS INT)] END AS epk
FROM part_raw;

CREATE OR REPLACE TEMPORARY VIEW pp AS
SELECT name, module, pos, pn AS param_name, paramValue[pos] AS pv, paramOldValue[pos] AS pov
FROM p LATERAL VIEW posexplode(paramName) t AS pos, pn;

-- Несколько партиций (3-7) и широкий диапазон (10-30)
CREATE OR REPLACE TEMPORARY VIEW range_narrow USING orc
OPTIONS (path '<INDEX_PATH>/key=<RANGE_KEYS>', basePath '<INDEX_PATH>');
CREATE OR REPLACE TEMPORARY VIEW rn AS
SELECT key, name, createdAt,
       CASE WHEN array_position(paramName, 'EPK_ID') > 0
            THEN paramValue[CAST(array_position(paramName, 'EPK_ID') - 1 AS INT)] END AS epk
FROM range_narrow;

CREATE OR REPLACE TEMPORARY VIEW range_wide USING orc
OPTIONS (path '<INDEX_PATH>/key=<RANGE_KEYS_WIDE>', basePath '<INDEX_PATH>');
CREATE OR REPLACE TEMPORARY VIEW rw AS
SELECT key, name, createdAt,
       CASE WHEN array_position(paramName, 'EPK_ID') > 0
            THEN paramValue[CAST(array_position(paramName, 'EPK_ID') - 1 AS INT)] END AS epk
FROM range_wide;


-- =====================================================================================================================
-- T1. ORC: Column pruning (очень высокий)
-- Проверка: при одном и том же scan Spark читает только необходимые колонки.
-- Метрика: bytes read в input metrics scan'а + время. Ожидаемо T1.1 < T1.2 < T1.3.
-- =====================================================================================================================
-- T1.1 узкая проекция (одна колонка + count)
SELECT count(*) AS rows_total, min(createdAt) AS min_createdAt, max(createdAt) AS max_createdAt
FROM p;

-- T1.2 средняя проекция (массивы параметров - основной объём данных)
SELECT count(*) AS rows_total,
       avg(size(paramName))  AS params_avg,
       avg(size(paramValue)) AS values_avg
FROM p;

-- T1.3 широкая проекция (все колонки, полная decode строки)
SELECT count(*) AS rows_total, avg(row_json_len) AS row_json_len_avg
FROM p;


-- =====================================================================================================================
-- T2. ORC: Predicate pushdown (очень высокий)
-- Проверка: фильтр на реальной ORC-колонке спускается в ORC reader (PushedFilters в EXPLAIN).
-- A/B: --conf spark.sql.orc.filterPushdown=false (выключить pushdown) против true (по умолчанию).
-- Контраст: epk - производная колонка (array_position), pushdown для неё невозможен (T2.5).
-- =====================================================================================================================
-- T2.1 равенство по колонке низкой кардинальности (доля <VAL_NAME> ~1%)
SELECT count(*) AS rows_returned FROM p WHERE name = '<VAL_NAME>';

-- T2.2 IN по колонке низкой кардинальности
SELECT count(*) AS rows_returned FROM p WHERE name IN ('<VAL_NAME>', '<VAL_NAME2>');

-- T2.3 временной диапазон ~1/3 партиции
SELECT count(*) AS rows_returned FROM p WHERE createdAt BETWEEN '<T_FROM>' AND '<T_TO>';

-- T2.4 равенство по колонке высокой кардинальности
SELECT count(*) AS rows_returned FROM p WHERE userLogin = '<VAL_LOGIN>';

-- T2.5 производная колонка: pushdown в ORC невозможен (контраст, базовый случай из 4.x spark_queries.sql)
SELECT count(*) AS rows_returned FROM p WHERE epk IS NOT NULL;


-- =====================================================================================================================
-- T3. ORC: min/max statistics, skipping stripe/row group (очень высокий)
-- Проверка: по статистике row group'ов (min/max) отбрасываются нерелевантные row group'ы.
-- A/B: --conf spark.sql.orc.rowGroupFilterEnabled=false против true (по умолчанию).
-- Метрика: строк прочитано (scan rows) vs строк возвращено. Эффективность зависит от
-- разброса времени внутри файла (раздел 7.2 spark_queries.sql): чем меньше span на файл, тем больше пропуска.
-- =====================================================================================================================
-- T3.1 узкий временной коридор (~1 час) по всей партиции
SELECT count(*) AS rows_returned FROM p WHERE createdAt BETWEEN '<T_NFROM>' AND '<T_NTO>';

-- T3.2 то же в пределах одного файла (изолирует row-group skipping внутри файла)
SELECT count(*) AS rows_returned
FROM p WHERE src_file = '<FILE_ORC>' AND createdAt BETWEEN '<T_NFROM>' AND '<T_NTO>';

-- T3.3 узкий коридор по __time
SELECT count(*) AS rows_returned FROM p WHERE `__time` BETWEEN '<T_NFROM>' AND '<T_NTO>';


-- =====================================================================================================================
-- T4. ORC: Bloom Filter, `=`/`IN` (высокий)
-- Проверка: row group'ы отбрасываются по bloom filter при равенстве/IN по колонкам, для которых
-- bloom filter записан в файл (высокая кардинальность: userLogin, session, userNode, archiveInternalId).
-- A/B: (а) тестовые данные с bloom filter и без (настройками writer'а);
--       (б) --conf spark.sql.orc.rowGroupFilterEnabled=false как нижняя граница (выключает min/max и bloom).
-- Наличие bloom filter в файле orc_meta.scala не показывает - проверить на стороне writer'а / ORC tools.
-- =====================================================================================================================
-- T4.1 равенство, высокая кардинальность
SELECT count(*) AS rows_returned FROM p WHERE userLogin = '<VAL_LOGIN>';

-- T4.2 IN, высокая кардинальность (5-10 значений)
SELECT count(*) AS rows_returned FROM p WHERE userLogin IN (<VAL_LOGINS>);

-- T4.3 равенство, низкая кардинальность (контроль: bloom filter мало эффективен)
SELECT count(*) AS rows_returned FROM p WHERE module = '<VAL_MODULE>';


-- =====================================================================================================================
-- T5. ORC: row.index.stride, детализация row-group index (средний/высокий)
-- Проверка: чем мельче индекс (меньше stride), тем точнее прицел фильтра на узкое условие
-- и тем меньше строки читается, но больше метаданных.
-- A/B: тестовые данные с rowIndexStride 1000 / 10000 / 100000 (writer; актуальное значение
-- выводит orc_meta.scala) + reader-настройка spark.sql.orc.rowIndexStride.
-- =====================================================================================================================
-- T5.1 точечный фильтр по высокой кардинальности в пределах одного файла
SELECT count(*) AS rows_returned FROM p WHERE src_file = '<FILE_ORC>' AND userLogin = '<VAL_LOGIN>';

-- T5.2 точечный фильтр по времени (1 минута) в пределах одного файла
SELECT count(*) AS rows_returned
FROM p WHERE src_file = '<FILE_ORC>' AND createdAt BETWEEN '<T_MIN_FROM>' AND '<T_MIN_TO>';


-- =====================================================================================================================
-- T6. ORC: сортировка данных, увеличение эффективности min/max (очень высокий)
-- Проверка: на отсортированных данных (createdAt / epk / name) диапазонные и точечные фильтры
-- пропускают row group'ы почти полностью; на неотсортированных - почти нет.
-- A/B: два тестовых датасета - (а) в текущей раскладке (как в проде, см. 7.1-7.2 spark_queries.sql),
--       (б) отсортированный по createdAt (и отдельно по epk).
-- Load: T2.1, T2.3, T3.1, T4.1 + запрос 8.1 из spark_queries.sql на каждом датасете.
-- Метрика: строк прочитано vs возвращено, время.
-- =====================================================================================================================
-- T6.1 (load на обоих датасетах)
SELECT count(*) AS rows_returned FROM p WHERE name = '<VAL_NAME>';

SELECT count(*) AS rows_returned FROM p WHERE createdAt BETWEEN '<T_FROM>' AND '<T_TO>';

SELECT count(*) AS rows_returned FROM p WHERE createdAt BETWEEN '<T_NFROM>' AND '<T_NTO>';

SELECT count(*) AS rows_returned FROM p WHERE userLogin = '<VAL_LOGIN>';


-- =====================================================================================================================
-- T7. ORC: stripe.size, размер единицы ORC scan (средний)
-- Проверка: крупный stripe - меньше метаданных/инициализаций, но хуже локализация фильтров;
-- мелкий - наоборот.
-- A/B: тестовые данные со stripe ~64MB и ~256MB (writer; результат проверять блоком
-- "размер stripe, байт" orc_meta.scala).
-- Load: T1.3 (широкий scan) + T2.3 (фильтр) + count(*) на каждом датасете.
-- Метрика: время, bytes read.
-- =====================================================================================================================


-- =====================================================================================================================
-- T8. ORC: compression, CPU <-> I/O компромисс (средний)
-- Проверка: сжатие снижает I/O, но тратит CPU на decode; выбор кодека под профиль нагрузки.
-- A/B: тестовые данные с spark.sql.orc.compression.codec = NONE / SNAPPY / ZLIB / ZSTD (writer).
-- Load: T1.3 (wide scan - decode) + T1.1 (I/O).
-- Метрика: время, Executor CPU Time, bytes read.
-- =====================================================================================================================


-- =====================================================================================================================
-- T9. HDFS/Table: Partitioning, исключение файлов/директорий (очень высокий)
-- Проверка: чтение одной партиции исключает остальные файлы/директории; стоимость перечисления
-- (listing basePath) растёт с числом партиций индекса.
-- A/B: 1 / 3 / 10-30 партиций (те же запросы на разных предпросмотрках).
-- Метрика: время, количество файлов в плане, время планирования (listing всего basePath).
-- =====================================================================================================================
-- T9.1 одна партиция
SELECT count(*) AS rows_total, min(createdAt) AS min_createdAt FROM p;

-- T9.2 3-7 партиций
SELECT count(*) AS rows_total, min(createdAt) AS min_createdAt FROM rn;

-- T9.3 10-30 партиций
SELECT count(*) AS rows_total, min(createdAt) AS min_createdAt FROM rw;


-- =====================================================================================================================
-- T10. HDFS/Table: Bucketing, JOIN/equality (выборочно высокий)
-- Проверка: bucket pruning по equality-фильтру (читается 1 из N файлов) и bucket join без shuffle.
-- Требуется: тестовые данные как таблица каталога, CLUSTERED BY (...) INTO N BUCKETS (T10.0).
-- A/B: bucketed таблица против обычных ORC-файлов в директории.
-- Метрика: количество файлов в плане (bucket pruning), shuffle в join'е.
-- =====================================================================================================================
-- T10.0 (шаг генерации данных, не выполняется раннером)
-- CREATE TABLE nt_bucketed (<колонки индекса>) STORED AS ORC
--   PARTITIONED BY (key STRING) CLUSTERED BY (userLogin) INTO 16 BUCKETS;
--
-- T10.1 equality-фильтр: bucket pruning (ожидаемо 1 из 16 файлов)
-- SELECT count(*) FROM nt_bucketed WHERE key='<PARTITION_KEY>' AND userLogin = '<VAL_LOGIN>';
--
-- T10.2 bucket join (обе таблицы bucketed по тому же ключу, одинаковое число buckets)
-- SELECT a.userLogin, count(*) AS joined_rows
-- FROM nt_bucketed a JOIN nt_bucketed_b b ON a.userLogin = b.userLogin
-- WHERE a.key='<PARTITION_KEY>' AND b.key='<PARTITION_KEY>'
-- GROUP BY a.userLogin ORDER BY joined_rows DESC LIMIT 100;


-- =====================================================================================================================
-- T11. HDFS: Compaction / small files, file-listing, scheduling, I/O (высокий)
-- Проверка: стоимость планирования и запуска растёт с числом файлов при том же числе строк.
-- A/B: тестовые данные с тем же числом строк: 1 файл / 10 файлов / 50 мелких (текущее состояние
-- прода - раздел 5 hdfs_stats.sh и 1.4 spark_queries.sql).
-- Load: T1.1 + T1.3.
-- Метрика: время планирования, количество задач, общее время.
-- =====================================================================================================================


-- =====================================================================================================================
-- T12. HDFS: Short-circuit read, локальный DataNode access (средний/высокий)
-- Проверка: чтение блока с локального диска вместо сети.
-- Load: T1.3 + T2.3 на мультинодовом кластере, executor'ы с locality к блокам партиции.
-- A/B: spark.hadoop.dfs.client.read.shortcircuit=true/false (+ spark.locality.wait).
-- Метрика: время; разница проявляется на кластере с несколькими DataNode.
-- =====================================================================================================================


-- =====================================================================================================================
-- T13. HDFS: Centralized Cache, повторные запросы (высокий)
-- Проверка: повторное выполнение того же запроса обслуживается из кеша, без перечитывания с HDFS.
-- A/B: (а) без кеша; (б) CACHE TABLE p (DISK_ONLY, см. стр. 63 spark_queries.sql);
--       (в) центральный кеш HDFS (если есть в стеке) - повторные запросы после первого "прогрева".
-- Load: T1.3 (широкий scan) выполнить 3 раза подряд; сравнить 1-й (cold) и 2-3-й (hot) запуски.
-- Метрика: время повторного запуска, hit ratio кеша.
-- =====================================================================================================================


-- =====================================================================================================================
-- T14. Spark 3.2: Native/vectorized ORC reader, decode/CPU (высокий)
-- Проверка: производительность decode ORC-колонк (CPU-bound нагрузка: широкие строки, длинные значения).
-- A/B: native ORC reader (если доступен в сборке Spark; имя настройки зависит от сборки -
-- проверить в SQLConf, в стандартном Apache Spark 3.2 vectorized reading включён по умолчанию)
-- против JVM-vectorized; дополнительно spark.sql.codegen.wholeStage=true/false.
-- Метрика: время, Executor CPU Time.
-- =====================================================================================================================
-- T14.1 decode широких строк (все колонки)
SELECT count(*) AS rows_total, avg(row_json_len) AS row_json_len_avg FROM p;

-- T14.2 decode тяжёлых строковых значений параметров (основной объём данных)
SELECT sum(length(pv)) AS chars_total, avg(length(pv)) AS chars_avg FROM pp;


-- =====================================================================================================================
-- T15. Spark 3.2: AQE, shuffle/join/skew (высокий)
-- Проверка: coalescing мелких shuffle-партаций, skew join, coalescing перед ORDER BY.
-- A/B: --conf spark.sql.adaptive.enabled=false против true (по умолчанию).
-- Метрика: количество shuffle-партаций (UI), время, дисбаланс задач (max/median task time).
-- =====================================================================================================================
-- T15.1 coalescing: explode даёт много мелких shuffle-партаций
SELECT param_name, count(*) AS occurrences
FROM pp GROUP BY param_name ORDER BY occurrences DESC LIMIT 100;

-- T15.2 skew join: топ-20 "тяжёлых" клиентов имеют кратно больше строк
SELECT h.epk, count(*) AS joined_rows
FROM (SELECT epk FROM p WHERE epk IS NOT NULL) f
JOIN (SELECT epk FROM (SELECT epk, count(*) AS c FROM p WHERE epk IS NOT NULL GROUP BY epk ORDER BY c DESC LIMIT 20) t) h
  ON f.epk = h.epk
GROUP BY h.epk ORDER BY joined_rows DESC;

-- T15.3 глобальный ORDER BY + LIMIT (аналог запроса 4.4 spark_queries.sql)
SELECT cnt, distinct_names
FROM (SELECT epk, count(*) AS cnt, count(DISTINCT name) AS distinct_names
      FROM p WHERE epk IS NOT NULL GROUP BY epk) t
ORDER BY cnt DESC LIMIT 50;


-- =====================================================================================================================
-- T16. Spark 3.2: DPP, partition pruning через JOIN (высокий)
-- Проверка: по небольшому dim-списку ключей партиций фактический scan читает только нужные
-- партиции, а не весь диапазон. Работает в составе AQE (нужен spark.sql.adaptive.enabled).
-- A/B: AQE on/off.
-- Метрика: количество файлов/партиций, реально прочитанных (UI, план scan) vs всего в диапазоне.
-- =====================================================================================================================
-- dim: набор нужных ключей партиций (2-3 из <RANGE_KEYS_WIDE>)
CREATE OR REPLACE TEMPORARY VIEW dim_keys AS
SELECT key FROM (SELECT explode(array(<DPP_KEY_LIST>)) AS key) t;

-- T16.1 fact (10-30 партиций) JOIN dim по ключу партиции
SELECT f.key, count(*) AS cnt, count(DISTINCT f.name) AS names
FROM rw f JOIN dim_keys d ON f.key = d.key
GROUP BY f.key ORDER BY f.key;


-- =====================================================================================================================
-- T17. Spark 3.2: CBO/statistics, выбор плана (средний/высокий)
-- Проверка: с таблицей каталога и статистикой CBO выбирает порядок join и join strategy.
-- Важно: для временных предпросмотров над директориями статистики отсутствуют - CBO неприменим,
-- тест выполняется только на таблицах каталога с тестовыми данными.
-- A/B: --conf spark.sql.cbo.enabled=false / true (+ spark.sql.cbo.joinReorder.enabled=true);
--      ANALYZE TABLE nt_fact COMPUTE STATISTICS FOR COLUMNS (name, userLogin); - до запуска.
-- Метрика: план (EXPLAIN COSTED), время.
-- =====================================================================================================================
-- T17.1 join "широкое имя события" x факт
-- SELECT f.name, count(*) AS cnt
-- FROM nt_fact f JOIN nt_dim_name d ON f.name = d.name
-- GROUP BY f.name ORDER BY cnt DESC LIMIT 100;


-- =====================================================================================================================
-- T18. Spark 3.2: Broadcast Join, исключение shuffle (высокий)
-- Проверка: малая сторона join'а broadcast'ится, shuffle отсутствует.
-- A/B: --conf spark.sql.autoBroadcastJoinThreshold=-1 (выключить) против дефолта.
-- Проверка плана: BroadcastHashJoin (A/B off: SortMergeJoin); Shuffle в UI отсутствует.
-- =====================================================================================================================
-- dim: распределение name по партиции (малая сторона, строки - справочные)
CREATE OR REPLACE TEMPORARY VIEW dim_name AS
SELECT name, count(*) AS base_cnt FROM p GROUP BY name;

-- T18.1 fact x dim по name
SELECT f.name, d.base_cnt, count(*) AS cnt
FROM p f JOIN dim_name d ON f.name = d.name
GROUP BY f.name, d.base_cnt
ORDER BY cnt DESC;
