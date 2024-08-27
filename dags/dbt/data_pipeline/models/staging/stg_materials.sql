
select
    *
from
    {{ source('postgre', 'MATERIALS') }}
