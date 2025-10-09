{{
    config(
        alias="veiculo",
        description="Dados brutos de veículos do SICOP (VW_VEICULO_DLK)"
    )
}}

select
  safe_cast(num_processo as string) as id_processo,
  safe_cast(num_placa as string)    as id_placa
from {{ source("brutos_sicop_staging","veiculo") }}
