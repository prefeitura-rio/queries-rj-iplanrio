{{
    config(
      schema="brutos_cadastro_comercio_ambulantes",
      alias="autorizacao",
      materialized="table",
      tags=["raw", "cadastro_comercio_ambulantes"],
      description="Dados do Titular da Autorização"
    )
}}

SELECT 
    safe_cast(id as integer) as id_autorizacao_titular,
    safe_cast(inscricaoMunicipal as string) as numero_inscricao_municipal,
    safe_cast(tipoPessoa as string) as tipo_pessoa,
    safe_cast(cpf as string) as cpf_titular,
    safe_cast(nome as string) as nome_titular_autorizacao,
    safe_cast(situacao as string) as situacao_inscricao_municipal,
    safe_cast(dt_concessao as datetime) as data_concessao,
    safe_cast(dt_cancelamento as datetime) as data_cancelamento,
    safe_cast(codigoLogradouroPonto as string) as endereco_corporativo_cod_logradouro_ponto,
    safe_cast(logradouroPonto as string) as endereco_corporativo_logradouro_ponto,
    safe_cast(referenciaPonto as string) as endereco_corporativo_referencia_ponto,
    safe_cast(numeroPortaPonto as string) as endereco_corporativo_num_porta_ponto,
    safe_cast(complementoPonto as string) as endereco_corporativo_complemento_ponto,
    safe_cast(codigoBairroPonto as string) as endereco_corporativo_cod_bairro_ponto,
    safe_cast(bairroPonto as string) as endereco_corporativo_bairro_ponto,
    safe_cast(equipamento as string) as nome_equipamento_autorizado,
    safe_cast(permanente as string) as indicador_autorizacao_permanente,
    safe_cast(processaConcessaoIM as string) as numero_processo_concessao,
    safe_cast(processoSeletivo as string) as numero_interno_agrupamento_concessao
FROM {{ source('brutos_cadastro_comercio_ambulantes_staging', 'vw_autorizacao_datalake') }}