-- SMAF
with question_blocks as (
select 
flow_id
, company_id
, revision_id
, content._id as flow_content_id
, content_blocks.value:_type::varchar(250) as content_block_type
, content_blocks.value as content_block_value_raw
, content_blocks.value:connected_variable_id::varchar(250) as connected_variable_id
, content_blocks.value:block_id::varchar(250) as block_id
, content_blocks.value:question:text::text as question_text
from MONGO_HONEYBOOK_PAYMENTS_PROD.flow_flows
join MONGO_HONEYBOOK_PAYMENTS_PROD.flow_flow_contents content on flow_flows._id = content.flow_id ,
LATERAL FLATTEN(content.blocks, recursive => false) as content_blocks
where 1=1
--and content.flow_id in ('625e2271a5fc6c00087696a5', '62617e08b7450f0008d31ffb', '62528dd9e502ab0006cc42ce', '6252967ee9e07c0007599870')
and company_id = '5edff70c9fd3b50d43a0e699'
and content_blocks.value:_type::varchar(250) ilike '%question%'
and content_blocks.value:question:text::text ilike '%phone color%'
),

variable as (
select
question_blocks.*
, flow_context_variable.value:variable_id::varchar(250) as variable_id
, flow_context_variable.value:connected_block_id::varchar(250) as connected_block_id
, flow_context_variable.value:answer::text as variable_answer
from question_blocks 
left join MONGO_HONEYBOOK_PAYMENTS_PROD.flow_revisions revision on question_blocks.flow_content_id = revision.flow_content_id and question_blocks.revision_id = revision._id ,
lateral flatten (revision.flow_context:variables, recursive=>false) as flow_context_variable
where 1=1 
and flow_context_variable.value:answer::text is not null
and question_blocks.connected_variable_id = flow_context_variable.value:variable_id::varchar(250)
) 

select 
variable.company_id
, variable.flow_id
, created_at
, booked_on
, project_id
, project_name
, max(case when question_text ilike '%What is the phone color you would like?%' then variable_answer else null end) as "What is the phone color you would like?"
, max(case when question_text ilike '%What is your second phone color choice if needed?%' then variable_answer else null end) as "What is your second phone color choice if needed?"
from variable
left join public.mongo_flow_file ff on variable.flow_id = ff.id
group by 1,2,3,4,5,6
order by created_at

-- legacy files
select files._id as file_id
, questions.value:question_text::varchar(250) as question_text
, questions.value:answer::varchar(250) as answer
from MONGO_HONEYBOOK_PAYMENTS_PROD.workspace_files files,
lateral flatten(files.questionnaire:questions , recursive => false) as questions
where 1=1
and account_id = '5c4366719f8ead3970c9baf4'
--and files._id = '6043b080deafe81431e10df5'
and _type = 'FileQuestionnaire'
and questions.value:question_text::varchar(250) ilike '%color%'