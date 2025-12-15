{{
    config(
        alias="veiculo",
        description="Dados brutos de veículos do SICOP (VW_VEICULO_DLK)"
    )
}}

select
  safe_cast(id_processo as string)                  as id_processo,
  safe_cast(id_placa as string)                     as id_placa,
  safe_cast(_prefect_extracted_at as date)          as datalake_transformed_at 
from {{ source("brutos_sicop_staging","veiculo") }}
