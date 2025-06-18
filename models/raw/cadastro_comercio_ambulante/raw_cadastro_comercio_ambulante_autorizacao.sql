{{
    config(
      schema="cadastro_comercio_ambulante",
      alias="autorizacao",
      materialized="table",
      tags=["raw", "cadastro_comercio_ambulante"],
      description="Dados do Titular da Autorização"
    )
}}

SELECT 
    SAFE_CAST(id as integer) as id_autoriz_titular,
    SAFE_CAST(inscricaoMunicipal as String) as num_inscricao_municipal,
    SAFE_CAST(tipoPessoa as String) as tipo_pessoa,
    SAFE_CAST(cpf as String) as cpf_titular,
    SAFE_CAST(nome as String) as nome_titular_autoriz,
    SAFE_CAST(situacao as String) as sit_inscricao_municipal,
    SAFE_CAST(dt_concessao as datetime) as dt_concessao,
    SAFE_CAST(dt_cancelamento as datetime) as dt_cancelamento,
    SAFE_CAST(codigoLogradouroPonto as String) as end_corporativo_cod_logradouro_ponto,
    SAFE_CAST(logradouroPonto as String) as end_corporativo_logradouro_ponto,
    SAFE_CAST(referenciaPonto as String) as end_corporativo_referencia_ponto,
    SAFE_CAST(numeroPortaPonto as String) as end_corporativo_num_porta_ponto,
    SAFE_CAST(complementoPonto as String) as end_corpotativo_compl_ponto,
    SAFE_CAST(codigoBairroPonto as String) as end_cod_bairro_ponto,
    SAFE_CAST(bairroPonto as String) as end_bairro_ponto,
    SAFE_CAST(equipamento as String) as nom_equip_autorizado,
    SAFE_CAST(permanente as String) as ind_autoriz_permanente,
    SAFE_CAST(processaConcessaoIM as String) as num_processo_concessao,
    SAFE_CAST(processoSeletivo as String) as num_interno_agrupam_concessao
FROM {{ source('brutos_cadastro_comercio_ambulante_staging', 'autorizacao') }}