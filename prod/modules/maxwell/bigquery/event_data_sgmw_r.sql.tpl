WITH column_metadata AS (
    -- Query to get the column count
    SELECT COUNT(column_name) AS column_count
    FROM ${project_id}.${dataset_id}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'raw_data_s3_new')

,source_s3 as (

    select JSON_EXTRACT_ARRAY(CONCAT('[', REPLACE(raw_data,'}{','},{'), ']')) json_table,  
   DATE(PARSE_TIMESTAMP('%Y/%m/%d', REGEXP_EXTRACT(file_name, '[0-9]{2,}/[0-9]+/[0-9]+'))) event_date,
   --(PARSE_TIMESTAMP('%Y-%m-%d-%I-%M-%S', REGEXP_EXTRACT(file_name, '[0-9]{2,}-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+'))) event_date,
    'sambla_group' as source, file_name, cast((select column_count  from  column_metadata) as string) as column_check
    from ${project_id}.${dataset_id}.${table_id_s3}
)
,
source_snowflake as (
    select database,table,type,ts,xid,xoffset,data data,
    date(TIMESTAMP_SECONDS(TS)) as event_date,
    'history' as source, cast(null as string) as file_name, '1' as column_check
    from ${project_id}.${dataset_id}.${table_id_snowflake}
    where date(TIMESTAMP_SECONDS(TS))>='2021-01-01' -- advisa history data before 2021
)

,flattened_table as
(
select 
    JSON_VALUE(v,'$.database') database,
    JSON_VALUE(v,'$.table') table,
    JSON_VALUE(v,'$.type') type,  
    CAST(JSON_VALUE(v,'$.ts') as INT64) ts,
    CAST(JSON_VALUE(v,'$.xid') as INT64) xid, 
    CAST(JSON_VALUE(v,'$.xoffset') as INT64) xoffset,
    JSON_QUERY(v,'$.data') data,
    event_date,
    source,  
    file_name,
    column_check
from source_s3 s, UNNEST(s.json_table) v
)
,union_sources as ( 
select * from flattened_table

union all
select * from source_snowflake

)
select * from union_sources
where true and DATE(TIMESTAMP_SECONDS(ts)) <= '2022-07-02'