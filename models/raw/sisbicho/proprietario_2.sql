with proprietario as (
    select * from {{ ref("proprietario_1") }}
)

select * from proprietario
