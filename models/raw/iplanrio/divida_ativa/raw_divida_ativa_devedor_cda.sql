{{
    config(
        alias="devedor_certidao_divida_ativa",
        materialized="table",
        tags=["raw", "devedor_cda", "cda", "devedor"],
        description="Tabela que contém os registros que vinculam os contribuintes com as certidões de dívida ativa - CDAs. Para efeito de identificação do devedor principal, considera-se o tipo de vínculo 'P'. Pode haver mais de um devedor principal para uma mesma CDA."
    )
}}

select safe_cast(numCDA as int64) as id_certidao_divida_ativa,
  safe_cast(idpessoa as int64) as id_pessoa,
  safe_cast(datsituacao as datetime) as data_criacao_registro,
  safe_cast(tipvinculo as string) as codigo_tipo_vinculo,
  case safe_cast(tipvinculo as string)
    when 'P' then 'Principal'
    when 'D' then 'Destinatário de correspondências'
    else 'Outros'
  end as nome_tipo_vinculo,
  safe_cast(descSituacaoDevedor as string) as cod_situacao_devedor,
  case safe_cast(descSituacaoDevedor as string)
    when '1' then 'Devedor Ativo'
    when '2' then 'Devedor Inativo'
    when '3' then 'Classificação indevida'
    else 'Não classificado'
  end as nome_situacao_devedor,
  _prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'DevedorCDA') }}