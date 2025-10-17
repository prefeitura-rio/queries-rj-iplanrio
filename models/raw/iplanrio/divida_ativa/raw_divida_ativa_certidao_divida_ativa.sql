{{
    config(
        alias="certidao_divida_ativa",
        materialized="table",
        tags=["raw", "divida_ativa", "certidao_divida_ativa", "CDA"],
        description="Tabela que contém os registros das Certidões Certidões de Dívida Ativa."
    )
}}

select safe_cast(a.numCDA as int64) as id_certidao_divida_ativa,
  safe_cast(a.anoInscricao as int64) as ano_de_inscricao_na_divida,
  safe_cast(a.seqCDA as int64) as sequencial_cda,
  safe_cast(a.numNotaDebito as numeric) as numero_nota_debito,
  safe_cast(a.codInscricaoImobiliaria as int64) as inscricao_imobiliaria_imovel_associado_iptu,
  safe_cast(a.codSituacaoCDA as int64) as id_situacao_cda,
  b.descricao_situacao_cda,
  safe_cast(a.idNaturezaDivida as int64) as id_natureza_divida,
  c.nome_natureza_divida,
  safe_cast(a.datCadastramento as date) as data_geracao_cda,
  safe_cast(a.DatSituacao as date) as data_ultima_alteracao_situacao,
  safe_cast(a.ProcessoAdm as string) as numero_processo_associado,
  safe_cast(a.codFaseCobranca as int64) as codigo_fase_cobranca,
  case safe_cast(a.codFaseCobranca as int64)
    when 1 then 'Amigável'
    when 2 then 'Judicial'
    when 3 then 'Amigável Protestado'
    when 4 then 'Judicial Protestado'
    else 'Não classificado'
  end as nome_fase_cobranca,
  safe_cast(a.ValSaldo as numeric) as valor_saldo_devido,
  safe_cast(a.ValMulta as numeric) as valor_multa_moratoria,
  safe_cast(a.ValJuros as numeric) as valor_juros_moratorios,
  safe_cast(a.ValMora as numeric) as valor_mora,
  safe_cast(a.ValMoraJuros as numeric) as valor_juros_mora,
  safe_cast(a.ValPagoPrinc as numeric) as valor_pago_principal,
  safe_cast(a.ValHonorarios as numeric) as valor_honorarios,
  safe_cast(a.valMoraOrigemSMF as numeric) as valor_mora_smf_iptu,
  safe_cast(a.valSaldoInscritoDA as numeric) as valor_original_divida_ativa,
  safe_cast(a.codReceita as string) as codigo_receita_cda,
  safe_cast(a.idEntidadeCredora as int64) as id_entidade_credora,
  case safe_cast(a.idEntidadeCredora as int64)
    when 1 then 'PCRJ'
    when 2 then 'PREVI-RIO'
    when 3 then 'FUNPREVI'
    else 'Não classificado'
  end as nome_entidade_credora,
  a._prefect_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_divida_ativa_staging_prefect', 'CDA') }} a
left join {{ ref('raw_divida_ativa_situacao_certidao_divida_ativa') }} b on CAST(b.id_situacao_cda as string) = CAST(a.codSituacaoCDA as string)
left join {{ ref('raw_divida_ativa_natureza_divida_ativa') }} c on CAST(c.id_natureza_divida as string) = a.idNaturezaDivida