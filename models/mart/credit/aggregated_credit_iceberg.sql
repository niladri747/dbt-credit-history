{{
    config(
        materialized='incremental',
        file_format='iceberg',
        location_root='s3://xldemo-feast-project-aws-bucket/iceberg/mart',
	partition_by=['event_timestamp'],
	incremental_strategy='append',
	schema= 'my_catalog.mart_credit'
    )
}}

with raw_credits as(
  select
    credit_card_due,
    mortgage_due,
    student_loan_due,
    vehicle_loan_due,
    event_timestamp
  from {{ ref('stg_credit_history') }}
),
 aggregated_credit as(
select
  sum(credit_card_due) as agg_credit_card_due,
  sum(mortgage_due) as agg_mortgage_due,
  sum(student_loan_due) as agg_student_loan_due,
  sum(vehicle_loan_due) as agg_vehicle_loan_due,
  to_date(event_timestamp) as event_timestamp
  from raw_credits
  group by event_timestamp
)
select
  aggregated_credit.agg_credit_card_due,
  aggregated_credit.agg_mortgage_due,
  aggregated_credit.agg_student_loan_due,
  aggregated_credit.agg_vehicle_loan_due,
  aggregated_credit.event_timestamp
from aggregated_credit


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where aggregated_credit.event_timestamp > (select max(event_timestamp) from {{ this }})

{% endif %}
