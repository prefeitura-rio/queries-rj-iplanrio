{{
    config(
        schema="brutos_divida_ativa",
        alias="honorario",
        materialized="table",
        tags=["raw", "divida_ativa", "honorário"],
        description="Tabela que contém os registros de guias de honorários vinculados aos cálculos das certidões de dívida ativa (CDA)"
    )
}}

select safe_cast(a.numCDA as int64) as id_certidao_divida_ativa,
  safe_cast(a.ValSaldo as numeric) as valor_saldo_devido, 
  safe_cast(a.valMM as numeric) as valor_multa_moratoria, 
  safe_cast(a.valJM as numeric) as valor_juros_moratorios, 
  safe_cast(a.valCM as numeric) as valor_correcao_monetaria, 
  safe_cast(a.valCMJ as numeric) as valor_correcao_monetaria_sobre_acrescimos,
  safe_cast(a.ValPago as numeric) as valor_pago, 
  safe_cast(a.percentualHonorarios as numeric) as percentual_honorarios,
  safe_cast(a.numSituacao as int64) as id_situacao_honorario,
  ifnull(safe_cast(nome_situacao_honorario as string), "Não classificado") as nome_situacao_honorario,
  a._airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at  
from {{ source('brutos_divida_ativa_staging', 'Honorarios') }} a
left join {{ ref('raw_divida_ativa_situacao_honorario') }} b on b.id_situacao_honorario = a.numSituacao