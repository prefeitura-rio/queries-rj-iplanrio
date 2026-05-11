{{
    config(
        alias="tratamento_fator",
        description="Tabela de Relacionamento entre Tratamento e Fator do sistema Vigia Urbano.",
    )
}}

SELECT 
    safe_cast(id AS int64) AS id_tratamento_fator,
    safe_cast(fatorid AS int64) AS id_fator,
    safe_cast(usuarioid AS int64) AS id_usuario,
    safe_cast((
        CASE 
            WHEN resolvido = "1" THEN TRUE
            ELSE FALSE
        END

    ) AS bool) AS flag_resolvido,
    safe_cast(justificativa AS string) AS justificativa,
    safe_cast(dataresolucao AS datetime) AS data_resolucao,
    safe_cast(caminhofoto AS string) AS caminho_foto,
    safe_cast(dataregistro AS datetime) AS data_registro,
    safe.parse_datetime('%Y-%m-%d %H:%M:%E*S', _prefect_extracted_at) as datalake_loaded_at,
    safe_cast(current_timestamp() as datetime) AS datalake_transformed_at
FROM {{ source('brutos_formulario_ocorrencia_staging', 'TratamentoFator') }}
