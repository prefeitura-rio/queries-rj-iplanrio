{{
    config(
      schema="brutos_sisbicho",
      alias="credenciada",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de credenciadas do sistema"
    )
}}

select
    safe_cast(IDCredenciada as integer) as id_credenciada,
    safe_cast(RazaoSocial as string) as razao_social_nome,
    safe_cast(CNPJ as string) as cnpj_numero,
    safe_cast(InscricaoMunicipal as string) as inscricao_municipal_numero,
    safe_cast(CodLogradouro as integer) as id_logradouro,
    safe_cast(Logradouro as string) as logradouro_nome,
    safe_cast(NumeroDePorta as string) as numero_porta,
    safe_cast(Complemento as string) as complemento_nome,
    safe_cast(Bairro as string) as bairro_nome,
    safe_cast(CodBairro as integer) as id_bairro,
    safe_cast(CEP as string) as cep_numero,
    safe_cast(Cidade as string) as cidade_nome,
    safe_cast(UF as string) as uf_sigla,
    safe_cast(Telefone as string) as telefone_numero,
    safe_cast(Celular as string) as celular_numero,
    safe_cast(Email as string) as email_endereco,
    safe_cast(DataRegistro as datetime) as registro_data,
    safe_cast(USR_LOGIN as string) as usuario_login,
    safe_cast(LicencaSanitaria as string) as licenca_sanitaria_numero,
    safe_cast(DataLicencaSanitaria as datetime) as licenca_sanitaria_data,
    safe_cast(CPF as string) as cpf_numero,
    safe_cast(IDTipoPessoa as string) as id_tipo_pessoa,
    safe_cast(IDResponsavelTecnico as integer) as id_responsavel_tecnico,
    safe_cast(TipoLicenca as string) as tipo_licenca_sigla,
    safe_cast(AutorizacaoSanitaria as string) as autorizacao_sanitaria_numero,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Credenciada') }} 