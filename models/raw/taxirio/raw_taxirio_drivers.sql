{{
    config(
        schema="brutos_taxirio",
        alias="motoristas",
        materialized="table",
        partition_by={
            "field": "data_criacao_particao",
            "data_type": "timestamp",
            "granularity": "day",
        },
        tags=["raw", "taxirio"],
        description="Tabela de Motoristas",
    )
}}

select
    safe_cast(id as string) as id_motorista,
    datetime(timestamp(createdat)) as data_criacao,
    datetime(timestamp(createdat)) as data_criacao_particao,
    safe_cast(user as string) as id_usuario,
    safe_cast(taxidriverid as string) as id_motorista_taxi,
    safe_cast(cars as string) as id_carro_taxi,
    safe_cast(average as float64) as nota_media,
    safe_cast(associatedcar as string) as id_carro_associado,
    safe_cast(status as string) as status,
    safe_cast(associateddiscount as string) as id_desconto_associado,
    parse_json(associatedpaymentsmethods) as pagamento_metodo,
    safe_cast(login as string) as usuario,
    safe_cast(password as string) as senha,
    safe_cast(salt as string) as dado_aleatorio,
    safe_cast(isabletoreceivepaymentinapp as bool) as pode_receber_pagamento_app,
    safe_cast(
        isabletoreceivepaymentincityhall as bool
    ) as pode_receber_pagamento_prefeitura,
    safe_cast(ratingsreceived as int64) as avaliacoes_recebidas,
    safe_cast(busy as bool) as ocupado,
    safe_cast(lastaverage as float64) as ultima_nota_media,
    datetime(
        timestamp(expiredblockbyrankingdate)
    ) as data_termino_classificacao_bloqueio,
    safe_cast(blockedrace as string) as id_corrida_bloqueada,
    safe_cast(city as string) as id_municipio,
    safe_cast(servicerecordrate as float64) as taxa_servico,
    safe_cast(nota as float64) as nota_passageiro,
    safe_cast(averagett as float64) as mediatt,
    datetime(timestamp(infophone_updatedat)) as atualizacao_telefone,
    safe_cast(infophone_id as string) as id_telefone,
    safe_cast(infophone_appversion as string) as versao_app_telefone,
    safe_cast(infophone_phonemodel as string) as modelo_telefone,
    safe_cast(infophone_phonemanufacturer as string) as fabricante_telefone,
    safe_cast(infophone_osversion as string) as versao_sistema_telefone,
    safe_cast(infophone_osname as string) as nome_sistema_telefone,
    safe_cast(tokeninfo_httpsalt as string) as ficha_http_aleatorio,
    safe_cast(tokeninfo_wsssalt as string) as ficha_wss_aleatorio,
    safe_cast(tokeninfo_pushtoken as string) as ficha_envio,
    safe_cast(
        associatedrace_originataccepted_position_lng as float64
    ) as corrida_origem_posicao_lng,
    safe_cast(
        associatedrace_originataccepted_position_lat as float64
    ) as corrida_origem_posicao_lat,
    safe_cast(associatedrace_race as string) as id_corrida,
    datetime(timestamp(expiredblockbyrankingdate)) as data_bloqueio_expirado,

from {{ source("brutos_taxirio_staging", "drivers") }}
