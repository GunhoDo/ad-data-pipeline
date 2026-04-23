PS C:\Users\Administrator>  aws s3 ls s3://s3-study-datalake/ad_lakehouse/events/ --recursive --profile second

2026-04-23 19:31:23        680 ad_lakehouse/events/data/fjinTQ/event_time_day=2026-04-11/20260423_103120_00025_xngec-03cb42d6-3217-4bef-b8cf-cb3080cceff3.parquet

2026-04-23 19:23:42        702 ad_lakehouse/events/data/iuCKew/event_time_day=2026-04-10/20260423_102341_00106_ydwqk-6a01d3c0-4cec-40d7-9776-a2e991e5b99e.parquet

2026-04-23 19:23:49        680 ad_lakehouse/events/data/lMfP4A/event_time_day=2026-04-11/20260423_102348_00070_ia3bg-7742a122-74d8-4a0f-8e26-9267041e755a.parquet

2026-04-23 19:20:04       1475 ad_lakehouse/events/metadata/00000-f3e44f97-2c80-42a5-8e38-bf215284730a.metadata.json

2026-04-23 19:23:42       2564 ad_lakehouse/events/metadata/00001-4dcff753-dbea-46cb-8b1d-6a226e74f8ff.metadata.json

2026-04-23 19:23:50       3593 ad_lakehouse/events/metadata/00002-3dd110ca-7bbc-4480-9e7a-f6f72a770f55.metadata.json

2026-04-23 19:31:23       4628 ad_lakehouse/events/metadata/00003-416db34a-198a-44dc-91c7-5af267af1c1c.metadata.json

2026-04-23 19:23:42       7142 ad_lakehouse/events/metadata/3b8ec5d7-35a9-4534-84ed-20aae4ef3b20-m0.avro

2026-04-23 19:23:49       7124 ad_lakehouse/events/metadata/3eccfc1b-d1d8-444b-9ca1-f289812c9c10-m0.avro

2026-04-23 19:31:23       7125 ad_lakehouse/events/metadata/a0b24b77-3923-45bc-a6a0-5432287730fb-m0.avro

2026-04-23 19:23:49       4350 ad_lakehouse/events/metadata/snap-458777870066505795-1-3eccfc1b-d1d8-444b-9ca1-f289812c9c10.avro

2026-04-23 19:23:42       4271 ad_lakehouse/events/metadata/snap-6762421301984409419-1-3b8ec5d7-35a9-4534-84ed-20aae4ef3b20.avro

2026-04-23 19:31:23       4402 ad_lakehouse/events/metadata/snap-7686945764465288512-1-a0b24b77-3923-45bc-a6a0-5432287730fb.avro

# 결과 해석
* 처음 세개의 parquet 파일은 sql query를 실행한 결과로서 저장되는 데이터 파일 입니다. parquet로 저장되는 것을 알 수 있고, query를 세번 실행 시킨 것을 알 수 있습니다
* 두번째부터 metadata/ 경로로 저장되는 것을 알 수 있는데 해당 파일은 Metadata File로서 .json 파일로 저장되는데 각 데이터는 스키마, 파티션 스펙, 스냅샷 목록, 현재 포인터 정보를 저장합니다
* 세번째 *-m0.avro 파일은 Manifest File로서 데이터 파일 목록 파일별 min/max, 통계 정보를 가지고 있을 것입니다. 
* 네번째 snap-*.avro파일은  Manifest List로서 현 스냅샷의 매니페스트 파일 목록 + 파티션 범위 요약하는 파일이고
>결과적으로 iceberg는 이러한 메타 데이터 구조로 기존의 datalake의 문제점을 해결하는 것을 알 수 있습니다.