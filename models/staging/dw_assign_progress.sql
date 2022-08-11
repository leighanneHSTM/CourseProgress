{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        schema = 'WAREHOUSE',
        unique_key='__id'
    )
}}
select 
    md5(source_org_node_id||ai_org_id||ai_assignment_instance_id||ai_user_student_id||ai_item_id||ai_item_version||ai_item_instance_id||ai_assignment_id) as __id,
    *
    --,
    --CURRENT_TIMESTAMP() AS CC_LOAD_TIMESTAMP
from {{ source('STG_COURSE_PROGRESS', 'STG_ASSIGN_PROGRESS') }}

where 1=1 

{% if is_incremental() %}
    and CC_LOAD_TIMESTAMP>(select max(CC_LOAD_TIMESTAMP) from {{this}})

{% endif %}