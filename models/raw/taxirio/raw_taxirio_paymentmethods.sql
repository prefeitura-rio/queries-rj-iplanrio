{{
    config(
        alias="metodos_pagamentos",
        description="Tabela de Pagamentos",
    )
}}
select
    safe_cast(id as string) as id_pagamento_associado,
    safe_cast(pindex as int64) as pindex,
    safe_cast(name as string) as nome,
    safe_cast(type as string) as tipo,
    _airbyte_extracted_at as datalake_loaded_at,    
    current_timestamp() as datalake_transformed_at   
from {{ source("brutos_taxirio_staging", "paymentmethods") }}
