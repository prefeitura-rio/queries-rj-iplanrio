{{
    config(
      alias="raca",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Raças de Animais"
    )
}}

select
    safe_cast(IDRaca as integer) as id_raca,
    safe_cast(NomeRaca as string) as raca_nome,
    safe_cast(IDEspecie as smallint) as id_especie,
    safe_cast(IDTipoEspecie as smallint) as id_tipo_especie,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Raca') }} 