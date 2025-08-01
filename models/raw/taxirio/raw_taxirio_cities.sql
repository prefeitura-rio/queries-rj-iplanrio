{{
    config(
        alias="cidades",
        description="Tabela de Cidades",
    )
}}

select
    safe_cast(id as string) as id_municipio,
    safe_cast(name as string) as nome_municipio,
    safe_cast(stateabbreviation as string) as abrev_municipio,
    safe_cast(isabletousepaymentinapp as bool) as pode_usar_pagamento_app,
    safe_cast(iscalulatedinapp as bool) as calculado_no_app,
    safe_cast(loginlabel as string) as forma_de_login,
    parse_json(servicestations) as estacoes_de_servico

from {{ source("brutos_taxirio_staging", "cities") }}
