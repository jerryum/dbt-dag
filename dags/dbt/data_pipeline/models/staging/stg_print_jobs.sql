
select
    *
from
    {{ source('postgre', 'PRINT_JOBS') }}
