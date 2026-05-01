# 4week_assignment.ipynb SQL 코드 정리

`4week_assignment.ipynb`에서 사용한 Spark SQL 코드만 뽑아서 실행 흐름대로 정리했다.  
전체 구조는 Raw 데이터를 Silver 테이블에 정리하고, 다시 Gold 테이블에서 캠페인별 통계로 집계하는 방식이다.

```text
Silver DDL
→ Silver MERGE INTO
→ Silver Snapshot
→ Gold DDL
→ Gold MERGE INTO
→ Gold Snapshot
→ 조회 및 통계 검증
```

---

## 1. Silver DDL 코드 및 해석

```sql
CREATE DATABASE IF NOT EXISTS local.ad_lakehouse
```

```sql
CREATE TABLE IF NOT EXISTS local.ad_lakehouse.processed_events (
    event_id STRING,
    event_date DATE,
    event_time TIMESTAMP,
    impression_timestamp BIGINT,
    impression_time TIMESTAMP,
    uid STRING,
    campaign INT,
    cost DOUBLE,
    clicked INT,
    conversion INT,
    conversion_delay_sec BIGINT,
    conversion_delay_min DOUBLE,
    conversion_delay_bucket STRING,
    is_late_conversion INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
)
```

Silver 테이블인 `processed_events`를 만드는 코드다.

이 테이블은 raw 영역에 따로 저장된 impression, click, conversion 데이터를 하나의 이벤트 단위로 정리하기 위한 테이블이다.  
기준은 `event_id`이고, 하나의 광고 노출 이벤트가 클릭과 전환까지 이어졌는지를 한 행에서 볼 수 있게 만든다.

여기서 중요한 설계 포인트는 **전환 지연 시간 분석 컬럼**이다.

- `conversion_delay_sec`
- `conversion_delay_min`
- `conversion_delay_bucket`

단순히 전환 여부만 저장하는 것이 아니라, 사용자가 광고를 본 뒤 얼마 만에 전환했는지를 같이 저장한다.  
그래서 나중에 “전환이 발생했는가?”뿐 아니라 “전환이 얼마나 빠르게 발생했는가?”까지 분석할 수 있다.

예를 들어 즉시 전환, 짧은 지연 전환, 긴 지연 전환을 구분하면 캠페인의 품질을 더 자세히 볼 수 있다.  
클릭률이나 전환율만 보면 놓칠 수 있는 시간 기반 성과를 보기 위한 설계라고 볼 수 있다.

`event_date`는 파티션 컬럼으로 사용한다. 날짜별 조회와 적재가 많기 때문에 전체 데이터를 매번 읽지 않도록 하기 위한 목적이다.

---

## 2. Silver MERGE INTO 코드 및 해석

```sql
MERGE INTO local.ad_lakehouse.processed_events AS target
USING (
    WITH impressions_dedup AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM parquet.`/home/jovyan/warehouse/raw/impressions`
        ) t
        WHERE rn = 1
    ),
    clicks_dedup AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM parquet.`/home/jovyan/warehouse/raw/clicks`
        ) t
        WHERE rn = 1
    ),
    conversions_dedup AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM parquet.`/home/jovyan/warehouse/raw/conversions`
        ) t
        WHERE rn = 1
    )
    SELECT
        i.event_id,
        CAST(to_timestamp(i.event_time) AS DATE) AS event_date,
        to_timestamp(i.event_time) AS event_time,
        i.timestamp AS impression_timestamp,
        to_timestamp(i.event_time) AS impression_time,
        i.uid,
        i.campaign,
        i.cost,
        CASE WHEN c.event_id IS NOT NULL THEN 1 ELSE 0 END AS clicked,
        CASE WHEN v.event_id IS NOT NULL THEN 1 ELSE 0 END AS conversion,
        v.conversion_delay_sec AS conversion_delay_sec,
        CASE
            WHEN v.conversion_delay_sec IS NULL THEN NULL
            ELSE CAST(v.conversion_delay_sec AS DOUBLE) / 60.0
        END AS conversion_delay_min,
        CASE
            WHEN v.conversion_delay_sec IS NULL THEN 'no_conversion'
            WHEN v.conversion_delay_sec < 60 THEN 'under_1min'
            WHEN v.conversion_delay_sec < 300 THEN '1_to_5min'
            WHEN v.conversion_delay_sec < 1800 THEN '5_to_30min'
            WHEN v.conversion_delay_sec < 3600 THEN '30min_to_1hour'
            ELSE 'over_1hour'
        END AS conversion_delay_bucket,
        CASE
            WHEN v.conversion_delay_sec IS NOT NULL AND v.conversion_delay_sec > 0 THEN 1
            ELSE 0
        END AS is_late_conversion,
        current_timestamp() AS updated_at
    FROM impressions_dedup i
    LEFT JOIN clicks_dedup c
        ON i.event_id = c.event_id
    LEFT JOIN conversions_dedup v
        ON i.event_id = v.event_id
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
    target.event_date = source.event_date,
    target.event_time = source.event_time,
    target.impression_timestamp = source.impression_timestamp,
    target.impression_time = source.impression_time,
    target.uid = source.uid,
    target.campaign = source.campaign,
    target.cost = source.cost,
    target.clicked = source.clicked,
    target.conversion = source.conversion,
    target.conversion_delay_sec = source.conversion_delay_sec,
    target.conversion_delay_min = source.conversion_delay_min,
    target.conversion_delay_bucket = source.conversion_delay_bucket,
    target.is_late_conversion = source.is_late_conversion,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    event_id,
    event_date,
    event_time,
    impression_timestamp,
    impression_time,
    uid,
    campaign,
    cost,
    clicked,
    conversion,
    conversion_delay_sec,
    conversion_delay_min,
    conversion_delay_bucket,
    is_late_conversion,
    created_at,
    updated_at
)
VALUES (
    source.event_id,
    source.event_date,
    source.event_time,
    source.impression_timestamp,
    source.impression_time,
    source.uid,
    source.campaign,
    source.cost,
    source.clicked,
    source.conversion,
    source.conversion_delay_sec,
    source.conversion_delay_min,
    source.conversion_delay_bucket,
    source.is_late_conversion,
    current_timestamp(),
    source.updated_at
)
```

이 코드는 raw 데이터를 읽어서 Silver 테이블에 upsert하는 부분이다.

먼저 impression, click, conversion 데이터를 각각 읽고, `event_id` 기준으로 최신 데이터만 남긴다.  
이를 위해 `ROW_NUMBER()`를 사용한다.

같은 `event_id`가 여러 번 들어올 수 있으므로, 중복 데이터를 그대로 넣지 않고 가장 최신 timestamp의 데이터만 사용한다.

그 다음 impression을 기준으로 click과 conversion을 `LEFT JOIN`한다.

```text
impression
   + click 여부
   + conversion 여부
   + conversion delay
```

`MERGE INTO`를 사용한 이유는 같은 이벤트가 다시 들어왔을 때 중복 insert를 막기 위해서다.

- 이미 있는 `event_id`면 UPDATE
- 없는 `event_id`면 INSERT

즉, Silver 테이블은 단순 적재 테이블이 아니라 이벤트 상태를 최신 상태로 유지하는 테이블이다.

---

## 3. Silver Snapshot 코드 및 해석

```sql
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM local.ad_lakehouse.processed_events.snapshots
ORDER BY committed_at DESC
LIMIT 5
```

Iceberg의 snapshot 정보를 확인하는 코드다.

Silver 테이블에 `MERGE INTO`가 실행되면 Iceberg는 새로운 snapshot을 만든다.  
이 쿼리를 통해 실제로 커밋이 발생했는지, 어떤 작업이 기록되었는지 확인할 수 있다.

여기서는 Silver 테이블이 정상적으로 변경되었는지 확인하는 용도다.

---

## 4. Gold DDL 코드 및 해석

```sql
CREATE TABLE IF NOT EXISTS local.ad_lakehouse.campaign_summary (
    summary_date DATE,
    campaign INT,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_cost DOUBLE,
    avg_conversion_delay_sec DOUBLE,
    avg_conversion_delay_min DOUBLE,
    ctr DOUBLE,
    conversion_rate DOUBLE,
    last_event_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
)
```

Gold 테이블인 `campaign_summary`를 만드는 코드다.

Silver가 이벤트 단위 테이블이라면, Gold는 분석용 집계 테이블이다.  
즉, `event_date`와 `campaign`을 기준으로 광고 성과를 요약한다.

Gold 테이블에서는 다음과 같은 지표를 저장한다.

- 노출 수
- 클릭 수
- 전환 수
- 총 비용
- CTR
- 전환율
- 평균 전환 지연 시간

여기서도 전환 지연 시간이 중요하다.  
Silver에서 만든 `conversion_delay_sec`, `conversion_delay_min`을 Gold에서 평균값으로 집계하면, 캠페인별로 전환이 얼마나 빠르게 일어나는지 비교할 수 있다.

단순히 전환 수가 많은 캠페인보다, 전환이 빠르게 일어나는 캠페인이 더 좋은 캠페인일 수도 있기 때문에 이 지표를 따로 둔 것이다.

---

## 5. Gold MERGE INTO 코드 및 해석

```sql
MERGE INTO local.ad_lakehouse.campaign_summary AS target
USING (
    SELECT
        event_date AS summary_date,
        campaign,
        COUNT(*) AS impressions,
        SUM(clicked) AS clicks,
        SUM(conversion) AS conversions,
        SUM(cost) AS total_cost,
        AVG(CASE WHEN conversion = 1 THEN conversion_delay_sec ELSE NULL END) AS avg_conversion_delay_sec,
        AVG(CASE WHEN conversion = 1 THEN conversion_delay_min ELSE NULL END) AS avg_conversion_delay_min,
        CASE
            WHEN COUNT(*) = 0 THEN 0.0
            ELSE CAST(SUM(clicked) AS DOUBLE) / CAST(COUNT(*) AS DOUBLE)
        END AS ctr,
        CASE
            WHEN SUM(clicked) = 0 THEN 0.0
            ELSE CAST(SUM(conversion) AS DOUBLE) / CAST(SUM(clicked) AS DOUBLE)
        END AS conversion_rate,
        MAX(updated_at) AS last_event_updated_at,
        current_timestamp() AS updated_at
    FROM local.ad_lakehouse.processed_events
    GROUP BY event_date, campaign
) AS source
ON target.summary_date = source.summary_date
AND target.campaign = source.campaign
WHEN MATCHED THEN UPDATE SET
    target.impressions = source.impressions,
    target.clicks = source.clicks,
    target.conversions = source.conversions,
    target.total_cost = source.total_cost,
    target.avg_conversion_delay_sec = source.avg_conversion_delay_sec,
    target.avg_conversion_delay_min = source.avg_conversion_delay_min,
    target.ctr = source.ctr,
    target.conversion_rate = source.conversion_rate,
    target.last_event_updated_at = source.last_event_updated_at,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    summary_date,
    campaign,
    impressions,
    clicks,
    conversions,
    total_cost,
    avg_conversion_delay_sec,
    avg_conversion_delay_min,
    ctr,
    conversion_rate,
    last_event_updated_at,
    created_at,
    updated_at
)
VALUES (
    source.summary_date,
    source.campaign,
    source.impressions,
    source.clicks,
    source.conversions,
    source.total_cost,
    source.avg_conversion_delay_sec,
    source.avg_conversion_delay_min,
    source.ctr,
    source.conversion_rate,
    source.last_event_updated_at,
    current_timestamp(),
    source.updated_at
)
```

Silver 테이블을 기준으로 Gold 테이블에 캠페인별 일별 성과를 반영하는 코드다.

집계 기준은 다음과 같다.

```sql
GROUP BY event_date, campaign
```

여기서 계산하는 값은 노출 수, 클릭 수, 전환 수, 총 비용, 클릭률, 전환율, 평균 전환 지연 시간이다.

Gold에서도 `MERGE INTO`를 사용하는 이유는 기존 집계 결과를 다시 계산해서 덮어쓸 수 있게 하기 위해서다.

- 같은 날짜와 캠페인이 있으면 UPDATE
- 없으면 INSERT

그래서 Silver 데이터가 바뀌거나 다시 적재되어도 Gold를 중복 없이 최신 집계 상태로 맞출 수 있다.

---

## 6. Gold Snapshot 코드 및 해석

```sql
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM local.ad_lakehouse.campaign_summary.snapshots
ORDER BY committed_at DESC
LIMIT 5
```

Gold 테이블의 Iceberg snapshot을 확인하는 코드다.

Gold `MERGE INTO`가 정상적으로 실행되었는지 확인하는 단계다.  
Silver와 마찬가지로 snapshot을 보면 테이블 변경 이력을 확인할 수 있다.

---

## 7. 조회 코드

### Silver 조회

```sql
SELECT
    event_id,
    event_date,
    uid,
    campaign,
    cost,
    clicked,
    conversion,
    conversion_delay_sec,
    conversion_delay_min,
    conversion_delay_bucket,
    is_late_conversion,
    created_at,
    updated_at
FROM local.ad_lakehouse.processed_events
ORDER BY event_id
LIMIT 20
```

Silver 테이블에 이벤트가 제대로 들어갔는지 샘플로 확인하는 코드다.  
특히 클릭 여부, 전환 여부, 전환 지연 시간 컬럼이 제대로 계산되었는지 확인할 수 있다.

### Gold 조회

```sql
SELECT
    summary_date,
    campaign,
    impressions,
    clicks,
    conversions,
    total_cost,
    avg_conversion_delay_sec,
    avg_conversion_delay_min,
    ctr,
    conversion_rate,
    last_event_updated_at,
    created_at,
    updated_at
FROM local.ad_lakehouse.campaign_summary
ORDER BY summary_date, campaign
LIMIT 20
```

Gold 테이블의 캠페인별 집계 결과를 확인하는 코드다.  
날짜와 캠페인별로 노출, 클릭, 전환, 비용, CTR, 전환율, 평균 전환 지연 시간을 확인한다.

---

## 8. 통계 검증 코드

```sql
WITH silver AS (
    SELECT
        COUNT(*) AS total_impressions,
        SUM(clicked) AS total_clicks,
        SUM(conversion) AS total_conversions,
        SUM(cost) AS total_cost
    FROM local.ad_lakehouse.processed_events
),
gold AS (
    SELECT
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions,
        SUM(total_cost) AS total_cost
    FROM local.ad_lakehouse.campaign_summary
)
SELECT
    silver.total_impressions AS silver_impressions,
    gold.total_impressions AS gold_impressions,
    silver.total_clicks AS silver_clicks,
    gold.total_clicks AS gold_clicks,
    silver.total_conversions AS silver_conversions,
    gold.total_conversions AS gold_conversions,
    silver.total_cost AS silver_total_cost,
    gold.total_cost AS gold_total_cost
FROM silver
CROSS JOIN gold
```

마지막으로 Silver와 Gold의 집계 결과가 맞는지 확인하는 코드다.

Gold는 Silver를 집계해서 만든 테이블이므로, 전체 기준으로 보면 다음 값들이 일치해야 한다.

```text
Silver 전체 이벤트 수 = Gold 노출 수 합계
Silver 클릭 수 합계 = Gold 클릭 수 합계
Silver 전환 수 합계 = Gold 전환 수 합계
Silver 비용 합계 = Gold 비용 합계
```

이 검증을 통해 Silver에서 Gold로 집계되는 과정에 누락이나 중복이 없는지 확인할 수 있다.

---

## 정리

이 노트북의 핵심은 raw 광고 이벤트를 Silver에서 이벤트 단위로 정리하고, Gold에서 캠페인별 성과로 집계하는 것이다.

특히 단순히 클릭/전환 여부만 보는 것이 아니라, `conversion_delay_sec`, `conversion_delay_min`, `conversion_delay_bucket`을 통해 전환이 발생하기까지 걸린 시간까지 분석할 수 있도록 설계한 점이 중요하다.
