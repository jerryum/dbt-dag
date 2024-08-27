{{ config(
    materialized='incremental',
    unique_key='material_id' 
) }}

select
    dim_materials.materialid as material_id,
    dim_materials.material as material_name,
    dim_materials.elongationmin as elongation_min,
    dim_materials.YIELDSTRENGTHMIN as yield_strength_min,
    dim_materials.ultimatetensilestrengthmin as ultimate_tensile_strength_min,
    dim_materials.created_at,
    dim_materials._airbyte_extracted_at as created_by_etl
from
    {{ref('stg_materials')}} as dim_materials
{% if is_incremental() %}
where
    created_at > (select max(created_at) from {{ this }})  
{% endif %}