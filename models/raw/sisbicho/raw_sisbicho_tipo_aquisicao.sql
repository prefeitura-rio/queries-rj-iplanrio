{{
    config(
      schema="brutos_sisbicho",
      alias="tipo_aquisicao",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Tipos de Aquisição de Animais do Sistema SisBicho"
    )
}}

SELECT 
    SAFE_cast([IDTipoAquisicao] as smallint) as id_tipo_aquisicao,
    SAFE_cast([Descricao] as string) as tipo_aquisicao_nome
FROM {{ source('brutos_sisbicho_staging', 'TipoAquisicao') }} 