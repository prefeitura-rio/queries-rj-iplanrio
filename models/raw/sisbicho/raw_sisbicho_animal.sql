{{
    config(
      schema="brutos_sisbicho",
      alias="animal",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Animais"
    )
}}

SELECT 
    SAFE_cast(IDAnimal as integer) as id_animal,
    SAFE_cast(Nome as string) as animal_nome,
    SAFE_cast(Microchip as string) as microchip_numero,
    SAFE_cast(IDEspecie as smallint) as id_especie,
    SAFE_cast(Sexo as string) as sexo_sigla,
    SAFE_cast(IDRaca as integer) as id_raca,
    SAFE_cast(DataNascimento as string) as nascimento_data,
    SAFE_cast(Pedigree as string) as pedigree_indicador,
    SAFE_cast(PedigreeOrigem as string) as pedigree_origem_nome,
    SAFE_cast(DataRegistro as datetime) as registro_data,
    SAFE_cast(IDPorte as integer) as id_porte,
    SAFE_cast(USR_LOGIN as string) as usuario_login,
    SAFE_cast(Fase as string) as fase_vida_nome,
    SAFE_cast(CodigoPedigree as string) as pedigree_codigo,
    SAFE_cast(IDCredenciada as integer) as id_credenciada,
    SAFE_cast(Motivo as string) as motivo_nome,
    SAFE_cast(Inativo as string) as inativo_indicador,
    SAFE_cast(IDTipoEspecie as smallint) as id_tipo_especie,
    SAFE_cast(DataAntirrabica as datetime) as antirrabica_data,
    SAFE_cast(DataAntirrabicaValidade as datetime) as antirrabica_validade_data,
    SAFE_cast(DataVermifugacao as datetime) as vermifugacao_data,
    SAFE_cast(DataVermifugacaoValidade as datetime) as vermifugacao_validade_data,
    SAFE_cast(QRCode as bytes) as qrcode_dados,
    SAFE_cast(Castrado as string) as castrado_indicador,
    SAFE_cast(Foto as bytes) as foto_dados
FROM {{ source('brutos_sisbicho_staging', 'Animal') }} 