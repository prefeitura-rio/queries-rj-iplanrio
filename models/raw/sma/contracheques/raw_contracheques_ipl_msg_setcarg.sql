{{
    config(
      alias="ipl_msg_setcarg",
      description="Tabela de ipl_msg_setcarg"
    )
}}

select
    safe_cast(MES_ANO_FOLHA as string) as mes_ano_folha,
    safe_cast(NUM_FOLHA as integer) as num_folha,
    safe_cast(CARGO as integer) as cargo,
    safe_cast(SETOR as integer) as setor,
    safe_cast(LINHA1 as integer) as linha1,
    safe_cast(LINHA2 as integer) as linha2,
    safe_cast(LINHA3 as integer) as linha3,
    safe_cast(LINHA4 as integer) as linha4,
    safe_cast(LINHA5 as integer) as linha5,
    safe_cast(_prefect_extracted_at as timestamp) as datalake_transformed_at
FROM {{ source('brutos_contracheque_staging', 'ipl_msg_setcarg') }}
