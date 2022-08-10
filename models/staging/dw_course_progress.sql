{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        schema = 'WAREHOUSE',
        unique_key='__id'
    )
}}
select 
    md5(cc_course_instance_id||cc_course_id||cc_course_version||cc_user_id||cc_user_student_id||source_org_node_id) as __id,
    *
    --,
    --CURRENT_TIMESTAMP() AS CC_LOAD_TIMESTAMP
from {{ source('STG_COURSE_PROGRESS', 'STG_COURSE_PROGRESS') }}

where 1=1 

{% if is_incremental() %}
    and CC_LOAD_TIMESTAMP>(select max(CC_LOAD_TIMESTAMP) from {{this}})

{% endif %}