{{
    config(
      alias="tipo_especie",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Espécies de Animais do Sistema SisBicho"
    )
}}

select
    safe_cast(IDTipoEspecie as smallint) as id_tipo_especie,
    safe_cast(NomeTipoEspecie as string) as tipo_especie_nome,
    safe_cast(IDEspecie as smallint) as id_especie,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'TipoEspecie') }} 