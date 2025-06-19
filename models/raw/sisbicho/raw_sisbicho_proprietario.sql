{{
    config(
      schema="brutos_sisbicho",
      alias="proprietario",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de proprietários de animais"
    )
}}

SELECT 
    SAFE_cast(IDProprietario as integer) as id_proprietario,
    SAFE_cast(Nome as string) as proprietario_nome,
    SAFE_cast(CPF as string) as cpf_numero,
    SAFE_cast(CNPJ as string) as cnpj_numero,
    SAFE_cast(CodLogradouro as integer) as id_logradouro,
    SAFE_cast(Logradouro as string) as logradouro_nome,
    SAFE_cast(NumeroDePorta as string) as numero_porta,
    SAFE_cast(Complemento as string) as complemento_nome,
    SAFE_cast(Bairro as string) as bairro_nome,
    SAFE_cast(CodBairro as integer) as id_bairro,
    SAFE_cast(CEP as string) as cep_numero,
    SAFE_cast(Cidade as string) as cidade_nome,
    SAFE_cast(UF as string) as uf_sigla,
    SAFE_cast(Telefone as string) as telefone_numero,
    SAFE_cast(Celular as string) as celular_numero,
    SAFE_cast(Email as string) as email_endereco,
    SAFE_cast(IDTipoPessoa as string) as tipo_pessoa_sigla,
    SAFE_cast(DataRegistro as datetime) as registro_datahora,
    SAFE_cast(USR_LOGIN as string) as usuario_login,
    SAFE_cast(DivulgarContato as string) as divulgar_contato_indicador,
    SAFE_cast(IDCredenciada as integer) as id_credenciada,
    SAFE_cast(TutorEmSituacaoDeRua as string) as tutor_rua_indicador
FROM {{ source('brutos_sisbicho_staging', 'Proprietario') }} 