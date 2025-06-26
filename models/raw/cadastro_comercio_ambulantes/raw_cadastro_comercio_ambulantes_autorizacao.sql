{{
    config(
      schema="cadastro_comercio_ambulantes",
      alias="autorizacao",
      materialized="table",
      tags=["raw", "cadastro_comercio_ambulantes"],
      description="Dados do Titular da Autorização"
    )
}}

SELECT 
    safe_cast(id as integer) as id_autoriz_titular,
    safe_cast(inscricaoMunicipal as string) as num_inscricao_municipal,
    safe_cast(tipoPessoa as string) as tipo_pessoa,
    safe_cast(cpf as string) as cpf_titular,
    safe_cast(nome as string) as nome_titular_autoriz,
    safe_cast(situacao as string) as sit_inscricao_municipal,
    safe_cast(dt_concessao as datetime) as dt_concessao,
    safe_cast(dt_cancelamento as datetime) as dt_cancelamento,
    safe_cast(codigoLogradouroPonto as string) as end_corporativo_cod_logradouro_ponto,
    safe_cast(logradouroPonto as string) as end_corporativo_logradouro_ponto,
    safe_cast(referenciaPonto as string) as end_corporativo_referencia_ponto,
    safe_cast(numeroPortaPonto as string) as end_corporativo_num_porta_ponto,
    safe_cast(complementoPonto as string) as end_corpotativo_compl_ponto,
    safe_cast(codigoBairroPonto as string) as end_cod_bairro_ponto,
    safe_cast(bairroPonto as string) as end_bairro_ponto,
    safe_cast(equipamento as string) as nom_equip_autorizado,
    safe_cast(permanente as string) as ind_autoriz_permanente,
    safe_cast(processaConcessaoIM as string) as num_processo_concessao,
    safe_cast(processoSeletivo as string) as num_interno_agrupam_concessao
FROM {{ source('brutos_cadastro_comercio_ambulantes_staging', 'vw_autorizacao_datalake') }}