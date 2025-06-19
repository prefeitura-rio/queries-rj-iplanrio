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
    SAFE_CAST(IDAnimalProprietario AS INT64) AS id_animal_proprietario,
    SAFE_CAST(IDAnimal AS INT64) AS id_animal,
    SAFE_CAST(IDProprietario AS INT64) AS id_proprietario,
    SAFE_CAST(IDTipoAquisicao AS INT64) AS id_tipo_aquisicao,
    SAFE_CAST(DataInicio AS DATETIME) AS inicio_datahora,
    SAFE_CAST(DataFim AS DATETIME) AS fim_datahora,
    SAFE_CAST(USR_CODIGO AS INT64) AS usuario_codigo,
    SAFE_CAST(DataCadastro AS DATETIME) AS cadastro_datahora
FROM {{ source('brutos_sisbicho_staging', 'AnimalProprietario') }}
