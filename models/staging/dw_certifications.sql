{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        schema = 'WAREHOUSE',
        unique_key='__id'
    )
}}
select 
    --md5(source_org_node_id||cert_org_id||cert_institution_org_node_id||username||certifying_body_name||certification_name||is_system_managed||expiration_datetime||certification_number||country_name||state_abbr||state_required||expiration_date_required||update_datetime) as __id,
    md5(source_org_node_id||cert_org_id||cert_institution_org_node_id||username||certifying_body_name||certification_name||is_system_managed||expiration_datetime||certification_number||country_name||state_abbr||state_required||expiration_date_required) as __id,
    *,
    --,
    CURRENT_TIMESTAMP() AS CC_LOAD_TIMESTAMP
from {{ source('STG_COURSE_PROGRESS', 'STG_CERTIFICATIONS') }}

where 1=1 

{% if is_incremental() %}
    and update_datetime>(select max(CC_LOAD_TIMESTAMP) from {{this}})

{% endif %}