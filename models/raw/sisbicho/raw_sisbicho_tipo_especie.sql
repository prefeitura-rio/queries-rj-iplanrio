{{
    config(
      schema="brutos_sisbicho",
      alias="tipo_especie",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Espécies de Animais do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_cast([IDTipoEspecie] as smallint) as id_tipo_especie,
    SAFE_cast([NomeTipoEspecie] as string) as tipo_especie_nome,
    SAFE_cast([IDEspecie] as smallint) as id_especie
FROM {{ source('brutos_sisbicho_staging', 'TipoEspecie') }} 