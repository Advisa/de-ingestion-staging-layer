CREATE OR REPLACE TABLE `${project_id}.${dataset_id}.${table_name}` AS
WITH applicants AS (
    SELECT 
        c.*
    EXCEPT (
        address,
        bank_iban,
        last_name,
        post_code,
        first_name,
        phone,
        email,
        ssn,
        city
    ),
    address,
    bank_iban,
    last_name,
    post_code,
    first_name,
    phone,
    email,
    ssn,
    city
    FROM 
        `${project_id}.${dataset_id}.${table_id}` a
    LEFT JOIN UNNEST(a.applicants) c ON a.data_id = c.data_id
),
employments AS (
    SELECT 
        applicant_id,
        ARRAY_AGG(
            (
                SELECT AS STRUCT ce.*
                EXCEPT (employer),
                employer
            )
        ) AS employments
    FROM 
        applicants c
    LEFT JOIN UNNEST(c.employments) ce ON c.id = ce.applicant_id
    GROUP BY 
        applicant_id
)
SELECT 
    a.*
    EXCEPT (employments),
    e.employments
FROM 
    applicants a
LEFT JOIN employments e ON e.applicant_id = a.id;


