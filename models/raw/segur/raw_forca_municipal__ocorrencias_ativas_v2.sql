{{
    config(
        schema='forca_municipal',
        alias="ocorrencias_ativas_v2",
        materialized='table',
    )
}}

SELECT
    SAFE_CAST(CreatedTime AS TIMESTAMP) AS data_hora_criacao,
    SAFE_CAST(AgencyEventId AS STRING) AS id_ocorrencia,
    SAFE_CAST(AgencyEventSubtypeCode AS STRING) AS codigo_subtipo_ocorrencia,
    SAFE_CAST(AgencyEventSubtypeCodeDesc AS STRING) AS descricao_subtipo_ocorrencia,
    SAFE_CAST(AgencyEventTypeCode AS STRING) AS codigo_tipo_ocorrencia,
    SAFE_CAST(AgencyEventTypeCodeDesc AS STRING) AS descricao_tipo_ocorrencia,
    SAFE_CAST(Area AS STRING) AS area,
    SAFE_CAST(Attributes AS INT64) AS atributos_flags,
    SAFE_CAST(Beat AS STRING) AS setor_patrulhamento,
    SAFE_CAST(ClosingComment AS STRING) AS comentario_encerramento,
    SAFE_CAST(ClosingEmployeeId AS STRING) AS id_funcionario_encerramento,
    SAFE_CAST(ClosingTime AS TIMESTAMP) AS data_hora_encerramento,
    SAFE_CAST(CreatedEmployeeId AS INT64) AS id_funcionario_criacao,
    SAFE_CAST(CustomData_Estado AS STRING) AS custom_data_estado,
    SAFE_CAST(DatabaseUpdateTime AS TIMESTAMP) AS data_hora_atualizacao_banco,
    SAFE_CAST(DispatchGroup AS STRING) AS grupo_despacho,
    SAFE_CAST(District AS STRING) AS bairro,
    SAFE_CAST(Esz AS INT64) AS codigo_zona_servico_emergencial,
    SAFE_CAST(EventDescription AS STRING) AS descricao_ocorrencia,
    SAFE_CAST(FirstUnitArrivedTime AS TIMESTAMP) AS data_hora_primeira_viatura_chegada,
    SAFE_CAST(FirstUnitDispatchedTime AS TIMESTAMP) AS data_hora_primeiro_despacho_viatura,
    SAFE_CAST(FirstUnitEnroutedTime AS TIMESTAMP) AS data_hora_primeiro_roteamento_viatura,
    SAFE_CAST(LastStatusChangeTime AS TIMESTAMP) AS data_hora_ultima_alteracao_status,
    SAFE_CAST(PrimaryEmployeeId AS STRING) AS id_funcionario_principal,
    SAFE_CAST(PrimaryUnitId AS STRING) AS id_viatura_principal,
    SAFE_CAST("Priority" AS INT64) AS nivel_prioridade,
    SAFE_CAST(PriorityChangedTime AS TIMESTAMP) AS data_hora_alteracao_prioridade,
    SAFE_CAST(StatusCode AS INT64) AS codigo_status,
    SAFE_CAST(UserGroupId AS INT64) AS id_grupo_usuarios,
    SAFE_CAST("Zone" AS STRING) AS zona_geografica,
    SAFE_CAST(Latitude AS FLOAT64) AS latitude,
    SAFE_CAST(Longitude AS FLOAT64) AS longitude,
    SAFE_CAST("Location" AS STRING) AS endereco,
    SAFE_CAST(Agents AS STRING) AS agentes_envolvidos
FROM {{ source('brutos_forca_municipal_staging', 'ocorrencias_ativas_v2') }}
