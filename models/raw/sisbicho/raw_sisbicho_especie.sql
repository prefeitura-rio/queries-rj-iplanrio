{{
    config(
      schema="brutos_sisbicho",
      alias="especie",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de espécies de animais"
    )
}}

SELECT 
    SAFE_cast(IDEspecie as smallint) as id_especie,
    SAFE_cast(NomeEspecie as string) as especie_nome,
    SAFE_cast(Avatar as bytes) as avatar_dados
FROM {{ source('brutos_sisbicho_staging', 'Especie') }} 