{{
    config(
        alias="unidade_administrativa",
        description="Dados brutos de unidades administrativas do município do Rio de Janeiro",
            )
}}


select 
    safe_cast(cd_ua as int64) as id_unidade_administrativa,
    safe_cast(sigla_ua as string) as sigla_unidade_administrativa,
    safe_cast(nome_ua as string) as nome_unidade_administrativa,
    safe_cast(cd_ua_basica as int64) as id_unidade_administrativa_basica,
    safe_cast(cd_ua_pai as int64) as id_unidade_administrativa_pai,
    safe_cast(ativa as string) as ativa,
    current_timestamp() as datalake_transformed_at
    
from {{ source("brutos_sici_staging", "Vw_UA") }}
