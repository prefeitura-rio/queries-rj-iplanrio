{{
    config(
      alias="animal_status",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de status dos animais"
    )
}}

select
    safe_cast(IDAnimalStatus as int64) as id_animal_status,
    safe_cast(IDStatus as int64) as id_status,
    safe_cast(IDAnimal as int64) as id_animal,
    safe_cast(Data as datetime) as registro_datahora,
    safe_cast(USR_CODIGO as int64) as usuario_codigo,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at 
from {{ source('brutos_sisbicho_staging', 'AnimalStatus') }} 