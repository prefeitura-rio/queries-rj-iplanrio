{{
    config(
        schema="brutos_divida_ativa",
        alias="devedor_certidao_divida_ativa",
        materialized="table",
        tags=["raw", "devedor_cda", "cda", "devedor"],
        description="Tabela que contém os registros que vinculam os contribuintes com as certidões de dívida ativa - CDAs. Para efeito de identificação do devedor principal, considera-se o tipo de vínculo 'P'. Pode haver mais de um devedor principal para uma mesma CDA."
    )
}}

select safe_cast(numCDA as int64) as id_certidao_divida_ativa,
  safe_cast(idpessoa as int64) as id_pessoa,
  safe_cast(datsituacao as date) as data_criacao_registro,
  safe_cast(tipvinculo as string) as codigo_tipo_vinculo,
  case safe_cast(tipvinculo as string)
    when 'P' then 'Principal'
    when 'D' then 'Destinatário de correspondências'
    else 'Outros'
  end as nome_tipo_vinculo,
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging', 'DevedorCDA') }}