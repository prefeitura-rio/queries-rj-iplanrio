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
    SAFE_cast([IDAnimalStatus] as integer) as id_animal_status,
    SAFE_cast([IDStatus] as integer) as id_status,
    SAFE_cast([IDAnimal] as integer) as id_animal,
    SAFE_cast([Data] as datetime) as registro_datahora,
    SAFE_cast([USR_CODIGO] as integer) as usuario_codigo
FROM {{ source('brutos_sisbicho_staging', 'AnimalStatus') }} 