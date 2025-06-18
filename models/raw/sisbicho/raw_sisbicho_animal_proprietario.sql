{{
    config(
      schema="brutos_sisbicho",
      alias="animal_proprietario",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de vínculos entre animais e proprietários"
    )
}}

SELECT 
    SAFE_cast([IDAnimalProprietario] as integer) as id_animal_proprietario,
    SAFE_cast([IDAnimal] as integer) as id_animal,
    SAFE_cast([IDProprietario] as integer) as id_proprietario,
    SAFE_cast([IDTipoAquisicao] as smallint) as id_tipo_aquisicao,
    SAFE_cast([DataInicio] as datetime) as inicio_datahora,
    SAFE_cast([DataFim] as datetime) as fim_datahora,
    SAFE_cast([USR_CODIGO] as integer) as usuario_codigo,
    SAFE_cast([DataCadastro] as datetime) as cadastro_datahora
FROM {{ source('brutos_sisbicho_staging', 'AnimalProprietario') }} 