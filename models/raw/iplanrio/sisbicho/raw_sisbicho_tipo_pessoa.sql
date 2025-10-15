{{
    config(
      alias="tipo_pessoa",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Pessoa do Sistema SisBicho"
    )
}}

select
    safe_cast(IDTipoPessoa as string) as id_tipo_pessoa,
    safe_cast(Descricao as string) as tipo_pessoa_nome,
    _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'TipoPessoa') }} 