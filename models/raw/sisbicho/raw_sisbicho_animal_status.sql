{{
    config(
      schema="brutos_sisbicho",
      alias="animal_status",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de status dos animais"
    )
}}

SELECT 
    SAFE_cast(IDAnimalStatus as INT64) as id_animal_status,
    SAFE_cast(IDStatus as INT64) as id_status,
    SAFE_cast(IDAnimal as INT64) as id_animal,
    SAFE_cast(Data as DATETIME) as registro_datahora,
    SAFE_cast(USR_CODIGO as INT64) as usuario_codigo
FROM {{ source('brutos_sisbicho_staging', 'AnimalStatus') }} 