{{ config(
    materialized='incremental',
    unique_key='print_jobs_test_results_id' 
) }}

select
print_jobs_test_results_id,
print_job_id,
print_job_date,
test_specimen_quantity,
print_job_created_at,
print_job_created_by_etl, 
test_result_id,
material_id,
invalid_test,
elongation,
yield_strength,
ultimate_tensile_strength,
tensile_specimen_id,
test_results_created_at,
test_results_created_by_etl,
created_at
from
    {{ref('int_print_jobs_test_results')}} as fact_print_jobs_test_results
{% if is_incremental() %}
where
    created_at > (select max(created_at) from {{ this }})  
{% endif %}