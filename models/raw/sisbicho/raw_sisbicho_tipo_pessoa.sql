{{
    config(
      schema="brutos_sisbicho",
      alias="tipo_pessoa",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Pessoa do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_cast([IDTipoPessoa] as string) as id_tipo_pessoa,
    SAFE_cast([Descricao] as string) as tipo_pessoa_nome
FROM {{ source('brutos_sisbicho_staging', 'TipoPessoa') }} 