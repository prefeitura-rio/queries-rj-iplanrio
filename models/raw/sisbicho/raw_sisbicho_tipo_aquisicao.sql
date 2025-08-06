{{
    config(
      alias="tipo_aquisicao",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") ,
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Aquisição de Animais do Sistema SisBicho"
    )
}}

select
    safe_cast(IDTipoAquisicao as smallint) as id_tipo_aquisicao,
    safe_cast(Descricao as string) as tipo_aquisicao_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'TipoAquisicao') }} 