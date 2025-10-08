{{
    config(
      alias="animal_proprietario",
      materialized="table",
      tags=["raw", "sisbicho"],
      description="Tabela de vínculos entre animais e proprietários"
    )
}}

select 
    safe_cast(IDAnimalProprietario as int64) as id_animal_proprietario,
    safe_cast(IDAnimal as int64) as id_animal,
    safe_cast(IDProprietario as int64) as id_proprietario,
    safe_cast(IDTipoAquisicao as int64) as id_tipo_aquisicao,
    safe_cast(DataInicio as datetime) as inicio_datahora,
    safe_cast(DataFim as datetime) as fim_datahora,
    safe_cast(USR_CODIGO as int64) as usuario_codigo,
    safe_cast(DataCadastro as datetime) as cadastro_datahora,
    --- _airbyte_extracted_at as datalake_loaded_at, 
    current_timestamp() as datalake_transformed_at
FROM {{ source('brutos_sisbicho_staging', 'AnimalProprietario') }}
