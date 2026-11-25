{{ config(alias='vw_clf_socio', materialized='table') }}

SELECT
    CAST(inscmunic AS STRING) AS inscmunic,
    CAST(cpf_cnpj AS STRING) AS cpf_cnpj,
    CAST(nome AS STRING) AS nome,
    CAST(qualificacao AS STRING) AS qualificacao,
    CAST(desc_qualificacao AS STRING) AS desc_qualificacao,
    CAST(identidade AS STRING) AS identidade,
    CAST(uf_identidade AS STRING) AS uf_identidade,
    CAST(orgao_emissor_identidade AS STRING) AS orgao_emissor_identidade,
    CAST(paticipacao AS STRING) AS paticipacao,
    CAST(cl AS STRING) AS cl,
    CAST(num_porta AS STRING) AS num_porta,
    CAST(complemento AS STRING) AS complemento,
    CAST(logradouro AS STRING) AS logradouro,
    CAST(bairro AS STRING) AS bairro,
    CAST(cep AS STRING) AS cep,
    CAST(municipio AS STRING) AS municipio,
    CAST(uf AS STRING) AS uf,
    CAST(pais AS STRING) AS pais
FROM {{ source('atividade_economica_sinae_staging', 'vw_clf_socio') }}
