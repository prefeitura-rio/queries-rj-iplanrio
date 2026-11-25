{{ config(alias='vw_clf_estabelecimento', materialized='table') }}

SELECT
    CAST(inscmunic AS STRING) AS inscmunic,
    CAST(cpf_cnpj AS STRING) AS cpf_cnpj,
    CAST(tipo_cpf_cnpj AS STRING) AS tipo_cpf_cnpj,
    CAST(nome AS STRING) AS nome,
    CAST(fantasia AS STRING) AS fantasia,
    CAST(tipo_estab AS STRING) AS tipo_estab,
    CAST(desc_tipo_estab AS STRING) AS desc_tipo_estab,
    CAST(status AS STRING) AS status,
    CAST(cep AS STRING) AS cep,
    CAST(cl AS STRING) AS cl,
    CAST(logradouro AS STRING) AS logradouro,
    CAST(numero AS STRING) AS numero,
    CAST(complemento AS STRING) AS complemento,
    CAST(cb AS STRING) AS cb,
    CAST(nm_bairro AS STRING) AS nm_bairro,
    CAST(ap AS STRING) AS ap,
    CAST(ativprinc AS STRING) AS ativprinc,
    CAST(data_alvara AS STRING) AS data_alvara,
    CAST(irlf AS STRING) AS irlf,
    CAST(zoneamento AS STRING) AS zoneamento,
    CAST(proc_concessao AS STRING) AS proc_concessao,
    CAST(data_deferimento AS STRING) AS data_deferimento,
    CAST(proc_deferimento AS STRING) AS proc_deferimento
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_estabelecimento') }}
