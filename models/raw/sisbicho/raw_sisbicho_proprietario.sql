{{
    config(
      alias="proprietario",
      project=("rj-iplanrio" if target.name == "prod" else "rj-iplanrio-dev") , 
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de proprietários de animais"
    )
}}

select
    safe_cast(IDProprietario as integer) as id_proprietario,
    safe_cast(Nome as string) as proprietario_nome,
    safe_cast(CPF as string) as cpf_numero,
    safe_cast(CNPJ as string) as cnpj_numero,
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
    safe_cast(IDTipoPessoa as string) as tipo_pessoa_sigla,
    safe_cast(DataRegistro as datetime) as registro_datahora,
    safe_cast(USR_LOGIN as string) as usuario_login,
    safe_cast(DivulgarContato as string) as divulgar_contato_indicador,
    safe_cast(IDCredenciada as integer) as id_credenciada,
    safe_cast(TutorEmSituacaoDeRua as string) as tutor_rua_indicador,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at
FROM {{ source('brutos_sisbicho_staging', 'Proprietario') }} 