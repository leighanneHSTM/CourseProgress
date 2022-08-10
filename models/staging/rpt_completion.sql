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
(select d.dm_user_last_name, d.dm_user_first_name, d.dm_user_email, d.dm_supervisor_name,
cp.cc_course_name, cp.cc_score, cp.cc_course_instance_status, cp.cc_completion_datetime
from cteDemo d
inner join cteCP cp
on d.dm_user_student_id=cc_user_student_id
)
select * from cteFinal
