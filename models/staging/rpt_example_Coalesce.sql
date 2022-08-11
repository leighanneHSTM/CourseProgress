{{config(
    query_tag = 'Lduvall_testingRpt',
    materialized = 'view',
    schema = 'WAREHOUSE'
)}}
with cteDemo as
(select *
from {{ source('STG_COURSE_PROGRESS', 'STG_DEMOGRAPHICS') }}
),
cteCP as
(select *
from {{ source('STG_COURSE_PROGRESS', 'STG_COURSE_PROGRESS') }}
),
cteFinal as
(select d.dm_user_last_name, d.dm_user_first_name, coalesce(count(cp.cc_course_instance_id),0) as num
from cteDemo d
left join cteCP cp
on d.dm_user_student_id=cc_user_student_id
group by d.dm_user_last_name, d.dm_user_first_name
)
select * from cteFinal
