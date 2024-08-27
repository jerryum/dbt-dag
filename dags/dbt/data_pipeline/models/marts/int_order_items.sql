select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.O_ORDERKEY as order_key,
    orders.O_CUSTKEY as cust_key,
    orders.O_ORDERDATE as order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.O_ORDERKEY = line_item.order_key
order by
    orders.O_ORDERDATE
