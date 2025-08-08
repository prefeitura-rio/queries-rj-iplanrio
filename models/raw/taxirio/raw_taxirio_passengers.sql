{{
    config(
        alias="passageiros",
        description="Tabela de Passageiros",
    )
}}

select

    safe_cast(id as string) as id_passageiro,
    safe_cast(user as string) as id_usuario,
    datetime(timestamp(createdat)) as data_criacao,
    safe_cast(login as string) as usuario,
    safe_cast(password as string) as senha,
    safe_cast(salt as string) as aleatorio,
    safe_cast(isabletousepaymentinapp as bool) as pode_usar_pagamento_app,
    datetime(timestamp(infophone_updatedat)) as data_atualizacao_telefone,
    safe_cast(infophone_id as string) as id_telefone,
    safe_cast(infophone_appversion as string) as versao_app_telefone,
    safe_cast(infophone_phonemodel as string) as modelo_telefone,
    safe_cast(infophone_phonemanufacturer as string) as fabricante_telefone,
    safe_cast(infophone_osversion as string) as versao_sistema_telefone,
    safe_cast(infophone_osname as string) as nome_sistema_telefone,
    safe_cast(tokeninfo_httpsalt as string) as ficha_http_aleatorio,
    safe_cast(tokeninfo_wsssalt as string) as ficha_wss_aleatorio,
    safe_cast(tokeninfo_pushtoken as string) as ficha_envio,
    _airbyte_extracted_at as datalake_loaded_at,    
    current_timestamp() as datalake_transformed_at     
from {{ source("brutos_taxirio_staging", "passengers") }}
