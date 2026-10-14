-- =====================================================================================================================
-- НТ: верификация механизмов Hive (vectorized execution, CBO/statistics, Tez, LLAP) на тех же данных.
--
-- Предусловие: тестовые данные загружены как EXTERNAL-таблица Hive (та же схема, что у индекса,
-- партицирована по key). Типы колонок сверить с column_profile.scala (в шаблоне - STRING/TIMESTAMP).
--
-- Запуск: beeline -e / -f, A/B-настройки через --hiveconf для каждого варианта:
--   V1 Vectorized execution : --hiveconf hive.vectorized.execution.enabled=true/false
--                             (и hive.vectorized.execution.reduce.enabled, hive.vectorized.groupby.enabled)
--   V2 CBO/statistics       : --hiveconf hive.cbo.enable=true/false
--                             (статистика: ANALYZE TABLE ... COMPUTE STATISTICS [FOR COLUMNS] перед A/B)
--   V3 Tez                  : --hiveconf hive.execution.mode=tez  против  hive.execution.mode=mr
--   V4 LLAP (Hive >= 2.0)   : --hiveconf hive.execution.mode=llap
--                             (повторные запросы: LLAP warm + ORC cache; hive.llap.io.* - политики IO)
--
-- Метрики: время запроса, intermediate data (Tez vs MR), строк прочитано, кеш-хиты LLAP.
-- Матрица: V1..V4 покрывают строки "Hive: Vectorized execution", "Hive: CBO/statistics",
-- "Hive: Tez", "Hive >= 2.0: LLAP" из общего списка механизмов.
-- =====================================================================================================================


-- ---------------------------------------------------------------------------------------------------------------------
-- 0. Таблица (один раз)
-- ---------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS nt_idx;
CREATE EXTERNAL TABLE nt_idx (
    name                STRING,
    module              STRING,
    userLogin           STRING,
    userName            STRING,
    session             STRING,
    userNode            STRING,
    metamodelVersion    STRING,
    tags                ARRAY<STRING>,
    paramName           ARRAY<STRING>,
    paramValue          ARRAY<STRING>,
    paramOldValue       ARRAY<STRING>,
    createdAt           TIMESTAMP,
    `__time`            TIMESTAMP,
    archiveInternalId   STRING,
    archiveInsertTime   TIMESTAMP
)
PARTITIONED BY (`key` STRING)
STORED AS ORC
LOCATION '<TABLE_PATH>';

MSCK REPAIR TABLE nt_idx;

-- Статистика для V2 (CBO): после создания таблицы
ANALYZE TABLE nt_idx PARTITION (`key`='<PARTITION_KEY>') COMPUTE STATISTICS;
ANALYZE TABLE nt_idx PARTITION (`key`='<PARTITION_KEY>') COMPUTE STATISTICS FOR COLUMNS (name, module, userLogin, userNode);


-- ---------------------------------------------------------------------------------------------------------------------
-- H1. I/O-нагрузка: полный scan одной партиции (partition pruning)
-- Входит в A/B: V1, V3, V4 (повторный запуск).
-- ---------------------------------------------------------------------------------------------------------------------
SELECT count(*) AS rows_total
FROM nt_idx WHERE `key`='<PARTITION_KEY>';


-- ---------------------------------------------------------------------------------------------------------------------
-- H2. Decode/CPU-нагрузка: все колонки (vectorized execution, ORC decode)
-- Входит в A/B: V1, V3, V4.
-- ---------------------------------------------------------------------------------------------------------------------
SELECT count(*) AS rows_total,
       avg(length(concat_ws(',', paramName)))  AS params_len_avg,
       avg(length(concat_ws(',', paramValue))) AS values_len_avg
FROM nt_idx WHERE `key`='<PARTITION_KEY>';


-- ---------------------------------------------------------------------------------------------------------------------
-- H3. Фильтры: predicate pushdown / min-max / bloom (то же, что T2-T4 в nt_perf_queries.sql)
-- Входит в A/B: V1, V3, V4 (повторные запуски для ORC-кеша LLAP).
-- ---------------------------------------------------------------------------------------------------------------------
SELECT count(*) AS rows_returned FROM nt_idx WHERE `key`='<PARTITION_KEY>' AND name = '<VAL_NAME>';

SELECT count(*) AS rows_returned FROM nt_idx WHERE `key`='<PARTITION_KEY>' AND name IN ('<VAL_NAME>', '<VAL_NAME2>');

SELECT count(*) AS rows_returned FROM nt_idx
WHERE `key`='<PARTITION_KEY>' AND createdAt BETWEEN '<T_NFROM>' AND '<T_NTO>';

SELECT count(*) AS rows_returned FROM nt_idx WHERE `key`='<PARTITION_KEY>' AND userLogin = '<VAL_LOGIN>';


-- ---------------------------------------------------------------------------------------------------------------------
-- H4. Агрегация со shuffle: explode + group by (vectorized reduce, coalescing в Tez)
-- Входит в A/B: V1, V3, V4.
-- ---------------------------------------------------------------------------------------------------------------------
SELECT pn AS param_name, count(*) AS occurrences
FROM nt_idx
LATERAL VIEW posexplode(paramName) t AS pos, pn
WHERE `key`='<PARTITION_KEY>'
GROUP BY pn
ORDER BY occurrences DESC
LIMIT 100;


-- ---------------------------------------------------------------------------------------------------------------------
-- H5. Join факт x dim (выбор join strategy: CBO, broadcast/SMJ; порядок: CBO joinReorder)
-- Входит в A/B: V2 (с/без статистики), V3, V4.
-- ---------------------------------------------------------------------------------------------------------------------
SELECT d.name, count(*) AS cnt
FROM nt_idx i
JOIN (SELECT DISTINCT name FROM nt_idx WHERE `key`='<PARTITION_KEY>') d ON i.name = d.name
WHERE i.`key`='<PARTITION_KEY>'
GROUP BY d.name
ORDER BY cnt DESC
LIMIT 100;


-- ---------------------------------------------------------------------------------------------------------------------
-- H6. Повторные запросы (LLAP: persistent execution + ORC cache, I/O)
-- Выполнить H1 + H3 три раза подряд в одном режиме (llap): 1-й запуск - прогрев,
-- 2-3-е - обслуживание из кеша. Сравнить со вторым прогоном в режиме tez (кеша LLAP нет).
-- ---------------------------------------------------------------------------------------------------------------------
