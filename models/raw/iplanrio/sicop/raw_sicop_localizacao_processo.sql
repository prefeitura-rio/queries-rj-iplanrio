{{
    config(
        alias="localizacao_processo",
        description="Dados brutos de localização física de processos do SICOP (VW_LOCALIZA_PROCESSO_DLK)"
    )
}}

-- Conversões e padronização de nomes conforme guia de estilo
select
  safe_cast(num_processo as string)                          as id_processo,
  safe_cast(localiza_processo as string)                     as id_localizacao_processo,
  safe_cast(_prefect_extracted_at as date)                   as datalake_transformed_at  
from {{ source('brutos_sicop_staging','localiza_processo') }}


