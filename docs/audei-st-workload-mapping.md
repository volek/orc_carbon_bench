# AUDEI / ST → orc-bench: mapping нагрузки

Связка требований из:

- [AUDEI Архитектура](AUDEI%20Архитектура-v180-20260916_113752.pdf) — доступ к журналам ЕРКЦ по `epk_id` + дате, SLA ≤ 3 с
- [Исследование запросов (стенд ST)](Исследование%20запросов%20для%20профиля%20нагрузки.pdf) — категории операторов Abyss/archive

с реализацией в **orc-bench** (факторный ORC harness на Spark/Hive).  
**Вне scope:** REST AUDEI, Abyss API, SOLR, Kafka, БД `audei_epk_data`.

---

## Профиль AUDEI (что моделируем)

| Требование AUDEI | Значение | В бенче |
|---|---|---|
| Ключ доступа | `epk_id` + интервал дат (типично ≤14 дней) | suite `audei`: `epk_eq_1d` / `epk_eq_14d` |
| Глубина хранения | 90 дней; поиска «сразу за 90 дней» нет | окна 1d / 14d / ≤31d, не full history |
| SLA клиентского API | ≤ **3 с** | `sla_class=interactive`, `--sla-threshold-ms=3000` |
| Нагрузка | ~9 TPS, запас ×2 → **18 QPS** | `run-concurrency.sh` уровни `9 18` |
| Ответ | ≤2000 сообщений, ≤10 МБ, сообщение ≤5 КБ | `rows_cap_ok` (≤2000); avg row ~1600 B |
| Кеш-выборка | день, `ORDER BY epk_id` | `order_by_epk_day` |
| Доп. фильтры | тип / канал / успешность | `eq_filters` (`name`, `channel_type`, `state`) |
| Paging RESULT | offset/limit ~1000 | `epk_page` LIMIT 1000 |

Prod объём ~2 ТБ/сутки — **не** цель стенда. Dataset **L ≤ 100 GB** (`--target-size-tb=0.1`) — масштаб кластера (~700 GB свободно на HDFS).

---

## Схема события

| Колонка ORC | Аналог AUDEI / Abyss |
|---|---|
| `epk_id` | root `epk_id` |
| `event_ts` | `__time` / `createdAt` |
| `name` | LOGON / FIND / LAUNCHER / ESA |
| `channel_type` | канал (Веб СБОЛ, МП, …) |
| `state` | успешность |
| `module`, `session`, `user_login` | атрибуты события |
| `payload_json` | JSON события (~1.6 КБ) |
| `event_year/month/day` | дневные партиции кеш-выборок |

Defaults записи: partition day, **sort `epk_id`**, **bloom `epk_id`**, `--avg-row-bytes=1600`.

---

## Suite CLI

| Alias | Назначение | Сценарии |
|---|---|---|
| `audei` | interactive SLA | `epk_eq_1d`, `epk_eq_14d`, `epk_page`, `eq_filters`, `order_by_epk_day` |
| `st` | archive / ST operator mix | `no_filter`, `like_single`, `like_multi`, `like_fulltext`, `eq`, `in_list`, `rlike` |
| `doc` | legacy Q1–Q10 (колонки AUDEI-mapped) | partition_prune … join_dictionary |
| `all` | всё сразу | |

Spark: `--benchmark-scenarios=audei`  
Hive: `SUITE=audei ./scripts/hive/run-hive-factor.sh h0`  
SQL-шаблоны: [scripts/hive/queries_audei_st.sql](../scripts/hive/queries_audei_st.sql)

---

## ST categories → scenarios

| ST `query_category` (доля ST) | Scenario | `sla_class` |
|---|---|---|
| `8_NO_FILTER` (~64%) | `no_filter` | archive |
| `3_LIKE_FULLTEXT` (~21%) | `like_fulltext` (≥10 LIKE) | archive |
| `5_LIKE_SINGLE` | `like_single` | archive |
| `4_LIKE_MULTI` | `like_multi` (5 LIKE) | archive |
| `7_EQ` | `eq`, `epk_eq_*`, `eq_filters` | archive / interactive |
| `6_IN` | `in_list` | archive |
| `2_RLIKE` (редко) | `rlike` | archive |

`duration_group`: окна ≤31 дня → `1_month` (91% ST-запросов).

Метрика ST: `seconds_per_gb` = `duration_sec / (bytes_read / 1 GiB)`.

---

## Dual SLA

| Класс | Порог | CLI | Когда |
|---|---|---|---|
| `interactive` | 3000 ms | `--sla-threshold-ms` | AUDEI API path |
| `archive` | 120000 ms | `--archive-sla-threshold-ms` | ST-like scans / LIKE fulltext |
| `legacy` | interactive | — | suite `doc` |

`order_by_epk_day` помечен `archive` (паттерн SELECT кеш-выборки, не 3 с API).

---

## Рекомендуемый порядок прогона

```text
smoke (audei, 10 GB)
  → B0 + SWEEP=audei на S (20 GB)     # partition / sort / bloom
  → best_orc freeze
  → Spark S0/S1 + Hive H0–H4
  → concurrency 9/18 на epk_eq_14d
  → L (100 GB) + run-sla-matrix.sh
  → опционально RUN_ST=1 для archive suite
```

Команды: [README.md](../README.md), [cluster_manual_runbook.md](cluster_manual_runbook.md),  
операционный протокол (порядок / HDFS / выгрузка): [cluster-run-protocol.md](cluster-run-protocol.md).

---

## Layout-факторы под AUDEI

Приоритет (`SWEEP=audei`):

1. Partition: `d0` none / `d1` day / `d2` +hour  
2. Sort: `e1` epk_id / `e2` event_id / `e3` event_ts+epk_id  
3. Bloom: `f0`–`f3`, FPP `g*`  

Вторично: stride / stripe / compression / file size (`SWEEP=secondary`).

Целевой `best_orc`: day + sort `epk_id` + bloom `epk_id` @ FPP 0.01.
