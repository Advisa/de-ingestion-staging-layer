with a as (
select ts,DATE(TIMESTAMP_SECONDS(ts)) timestamp_ts ,id,
json_query(json_col,'$.soap:Envelope.soap:Body.ns2:ucReply.ns2:ucReport.ns2:xmlReply.ns2:reports.ns2:report') as reports,
json_col 
from {{ref('credit_reports_xml_extract_sgmw_r')}}
),

b as (
select ts,timestamp_ts,id,
json_value(reports,'$.ns2:index') as report_index,
json_value(reports,'$.ns2:styp') as report_styp,
json_value(reports,'$.ns2:name') as report_name,
json_value(reports,'$.ns2:id') as report_pid,
JSON_EXTRACT_ARRAY(reports,'$.ns2:group') as grp,

from a),

c as (
select  b.* except(grp),
json_value(grp,'$.ns2:name') as grp_name,
json_value(grp,'$.ns2:key') as grp_key,
json_value(grp,'$.ns2:index') as grp_index,
json_value(grp,'$.ns2:id') as grp_id,
JSON_EXTRACT_ARRAY(grp,'$.ns2:term') as term,

from b, unnest(b.grp) as grp),

d as (
select c.* except(term),
json_value(term,'$.ns2:id') as term_id,
json_value(term,'$.text') as term_text,

from c,unnest(c.term) as term
)

select * from d
where True