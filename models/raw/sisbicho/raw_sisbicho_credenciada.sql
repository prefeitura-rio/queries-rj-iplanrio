{{
    config(
      schema="brutos_sisbicho",
      alias="credenciada",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de credenciadas do sistema"
    )
}}

SELECT 
    SAFE_cast([IDCredenciada] as integer) as id_credenciada,
    SAFE_cast([RazaoSocial] as string) as razao_social_nome,
    SAFE_cast([CNPJ] as string) as cnpj_numero,
    SAFE_cast([InscricaoMunicipal] as string) as inscricao_municipal_numero,
    SAFE_cast([CodLogradouro] as integer) as id_logradouro,
    SAFE_cast([Logradouro] as string) as logradouro_nome,
    SAFE_cast([NumeroDePorta] as string) as numero_porta,
    SAFE_cast([Complemento] as string) as complemento_nome,
    SAFE_cast([Bairro] as string) as bairro_nome,
    SAFE_cast([CodBairro] as integer) as id_bairro,
    SAFE_cast([CEP] as string) as cep_numero,
    SAFE_cast([Cidade] as string) as cidade_nome,
    SAFE_cast([UF] as string) as uf_sigla,
    SAFE_cast([Telefone] as string) as telefone_numero,
    SAFE_cast([Celular] as string) as celular_numero,
    SAFE_cast([Email] as string) as email_endereco,
    SAFE_cast([DataRegistro] as datetime) as registro_data,
    SAFE_cast([USR_LOGIN] as string) as usuario_login,
    SAFE_cast([LicencaSanitaria] as string) as licenca_sanitaria_numero,
    SAFE_cast([DataLicencaSanitaria] as datetime) as licenca_sanitaria_data,
    SAFE_cast([CPF] as string) as cpf_numero,
    SAFE_cast([IDTipoPessoa] as string) as id_tipo_pessoa,
    SAFE_cast([IDResponsavelTecnico] as integer) as id_responsavel_tecnico,
    SAFE_cast([TipoLicenca] as string) as tipo_licenca_sigla,
    SAFE_cast([AutorizacaoSanitaria] as string) as autorizacao_sanitaria_numero
FROM {{ source('brutos_sisbicho_staging', 'Credenciada') }} 