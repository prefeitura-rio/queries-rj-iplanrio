{{
    config(
        schema="brutos_mais_valia",
        alias="parcelas_contrapartida",
        materialized="table",
        tags=["raw", "mais_valia", "mais", "valia", "contrapartida", "parcelas"],
        description="Tabela que espelha a view vwCTP_darm_ctp que consiste nos dados básicos das parcelas do cálculo de contrapartida, mesmo aquelas que ainda não tiveram o número de DARM gerado."
    )
}}

select
    safe_cast(origem as string) || "-" || safe_cast(codigo_calculo as string) as id_calculo,
    safe_cast(origem as string) || "-" || safe_cast(codigo_calculo as string) || '-' || safe_cast(codigo_parcela as string) as id_parcela,
    safe_cast(origem as int64) as origem,
    safe_cast(codigo_calculo as int64) as codigo_calculo,
    safe_cast(codigo_parcela as int64) as codigo_parcela,
    safe_cast(numero_parcela as int64) as numero_parcela,
    safe_cast(numero_darm as int64) as numero_darm,
    safe_cast(darm_emitido as boolean) as darm_emitido,
    case safe_cast(darm_emitido as boolean)
        when true then "Emitido"
        when false then "Não Emitido"
        else null
    end as descricao_darm_emitido,
    safe_cast(valor as numeric) as valor,
    safe_cast(data_emissao as date) as data_emissao,
    safe_cast(data_vencimento as date) as data_vencimento,
    safe_cast(data_pagamento as date) as data_pagamento,
    safe_cast(valor_pagamento as numeric) as valor_pagamento,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_mais_valia_staging', 'vwCTP_darm_ctp') }}
where codigo_calculo is not null
and data_vencimento is not null