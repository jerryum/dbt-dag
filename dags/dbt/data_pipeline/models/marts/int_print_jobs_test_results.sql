

select
{{
    dbt_utils.generate_surrogate_key([
        'print_jobs.printjobid',
        'test_results.testresultid'
    ])
}} as print_jobs_test_results_id,
print_jobs.printjobid as print_job_id,
print_jobs.printjobdate as print_job_date,
print_jobs.testspecimenquantity as test_specimen_quantity,
print_jobs.created_at as print_job_created_at,
print_jobs._airbyte_extracted_at as print_job_created_by_etl, 
test_results.testresultid as test_result_id,
test_results.materialid as material_id,
test_results.invalidtest as invalid_test,
test_results.elongation,
test_results.yieldstrength as yield_strength,
test_results.ultimatetensilestrength as ultimate_tensile_strength,
test_results.tensilespecimenid as tensile_specimen_id,
test_results.created_at as test_results_created_at,
test_results._airbyte_extracted_at as test_results_created_by_etl,
CURRENT_TIMESTAMP() as created_at
from
    {{ ref('stg_print_jobs') }} as print_jobs
join
    {{ ref('stg_test_results') }} as test_results
        on print_jobs.printjobid = test_results.printjobid
order by
    print_jobs_test_results_id
