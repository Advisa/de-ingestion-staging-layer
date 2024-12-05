CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.${table_name}`
    PARTITION BY DATE(event_date) 
    CLUSTER BY `table`
    AS
with source_s3 as (

    select JSON_EXTRACT_ARRAY(CONCAT('[', REPLACE(raw_data,'}{','},{'), ']')) json_table,  
   DATE(PARSE_TIMESTAMP('%Y/%m/%d', REGEXP_EXTRACT(_FILE_NAME, '[0-9]{2,}/[0-9]+/[0-9]+'))) event_date,
   --(PARSE_TIMESTAMP('%Y-%m-%d-%I-%M-%S', REGEXP_EXTRACT(_FILE_NAME, '[0-9]{2,}-[0-9]+-[0-9]+-[0-9]+-[0-9]+-[0-9]+'))) event_date,
    'sambla_group' as source, _FILE_NAME file_name, '{{get_metadata}}' as column_check
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