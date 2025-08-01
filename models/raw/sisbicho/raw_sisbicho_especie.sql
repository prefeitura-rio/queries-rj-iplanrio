{{
    config(
      schema="brutos_sisbicho",
      alias="especie",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de espécies de animais"
    )
}}

select
    safe_cast(IDEspecie as smallint) as id_especie,
    safe_cast(NomeEspecie as string) as especie_nome,
    safe_cast(Avatar as bytes) as avatar_dados,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Especie') }} 