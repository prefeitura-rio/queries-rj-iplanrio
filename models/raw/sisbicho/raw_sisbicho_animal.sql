{{
    config(
      schema="brutos_sisbicho",
      alias="animal",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de Animais",
      partition_by={
            "field": "nascimento_data",
            "data_type": "datetime",
            "granularity": "month"
       }
    )
}}

select
    safe_cast(IDAnimal as integer) as id_animal,
    safe_cast(Nome as string) as animal_nome,
    safe_cast(Microchip as string) as microchip_numero,
    safe_cast(IDEspecie as smallint) as id_especie,
    safe_cast(Sexo as string) as sexo_sigla,
    safe_cast(IDRaca as integer) as id_raca,
    safe_cast(DataNascimento as datetime) as nascimento_data,
    safe_cast(Pedigree as string) as pedigree_indicador,
    safe_cast(PedigreeOrigem as string) as pedigree_origem_nome,
    safe_cast(DataRegistro as datetime) as registro_data,
    safe_cast(IDPorte as integer) as id_porte,
    safe_cast(USR_LOGIN as string) as usuario_login,
    safe_cast(Fase as string) as fase_vida_nome,
    safe_cast(CodigoPedigree as string) as pedigree_codigo,
    safe_cast(IDCredenciada as integer) as id_credenciada,
    safe_cast(Motivo as string) as motivo_nome,
    safe_cast(Inativo as string) as inativo_indicador,
    safe_cast(IDTipoEspecie as smallint) as id_tipo_especie,
    safe_cast(DataAntirrabica as datetime) as antirrabica_data,
    safe_cast(DataAntirrabicaValidade as datetime) as antirrabica_validade_data,
    safe_cast(DataVermifugacao as datetime) as vermifugacao_data,
    safe_cast(DataVermifugacaoValidade as datetime) as vermifugacao_validade_data,
    safe_cast(QRCode as bytes) as qrcode_dados,
    safe_cast(Castrado as string) as castrado_indicador,
    safe_cast(Foto as bytes) as foto_dados,
    _airbyte_extracted_at as loaded_at, 
    current_timestamp() as transformed_at   
FROM {{ source('brutos_sisbicho_staging', 'Animal') }} 