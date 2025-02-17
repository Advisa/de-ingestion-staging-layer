WITH raw_data AS (
    SELECT 
        *, 
        DATE(CONCAT(
            SPLIT(file_name, '/')[OFFSET(4)], "-", 
            SPLIT(file_name, '/')[OFFSET(5)], "-", 
            SPLIT(file_name, '/')[OFFSET(6)]
        )) AS dedupe_date 
    FROM (
        SELECT *, _FILE_NAME AS file_name 
        FROM `${project_id}.${dataset_id}.providers_lvs_r`
    )
), 

main AS (
    SELECT * EXCEPT(file_name) 
    FROM raw_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY dedupe_date DESC) = 1
)

SELECT 
    'approved' IN UNNEST(cpl_rules) AS cpl_approved,
    'APIapproved' IN UNNEST(cpl_rules) AS cpl_api_approved,
    'paid' IN UNNEST(cpl_rules) AS cpl_paid,
    * EXCEPT(cpl_rules, commissions) 
FROM main;