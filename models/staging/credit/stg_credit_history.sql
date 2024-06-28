select
dob_ssn,
credit_card_due,
    mortgage_due,
    student_loan_due,
    vehicle_loan_due,
    event_timestamp

from {{ source('raw_credit_history','credit_history') }};
