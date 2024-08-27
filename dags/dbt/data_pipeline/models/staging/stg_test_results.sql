
select
    *
from
    {{ source('postgre', 'TEST_RESULTS') }}
