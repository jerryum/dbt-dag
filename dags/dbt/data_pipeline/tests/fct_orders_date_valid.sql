select
    *
from
    {{ref('fct_orders')}}
where
    date(o_orderdate) > CURRENT_DATE()
    or date(o_orderdate) < date('1990-01-01')