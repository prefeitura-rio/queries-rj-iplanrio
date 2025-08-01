{{
    config(
        schema="brutos_taxirio",
        alias="metodos_pagamentos",
        materialized="table",
        tags=["raw", "taxirio"],
        description="Tabela de Pagamentos",
    )
}}
select
    safe_cast(id as string) as id_pagamento_associado,
    safe_cast(pindex as int64) as pindex,
    safe_cast(name as string) as nome,
    safe_cast(type as string) as tipo
from {{ source("brutos_taxirio_staging", "paymentmethods") }}
