select 
  *
FROM
    {{ source('tpch', 'orders') }}