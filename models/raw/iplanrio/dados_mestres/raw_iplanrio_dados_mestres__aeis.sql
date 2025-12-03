{{
    config(
        alias="aeis",
        description="Dados de Áreas de Especial Interesse Social (AEIS) do município do Rio de Janeiro"
    )
}}

SELECT 
    SAFE_CAST(cod AS STRING) id_aeis, 
    SAFE_CAST(nome AS STRING) nome_aeis,
    SAFE_CAST(tipologia AS STRING) tipologia,
    SAFE_CAST(item AS STRING) item,
    SAFE_CAST(setor AS STRING) nome_setor,
    SAFE_CAST(sigla AS STRING) nome_sigla,
    SAFE_CAST(nome_do_ar AS STRING) nome_ar, # verificar metadado
    SAFE_CAST(planta_cad AS STRING) planta_cadastral,
    SAFE_CAST(ra AS STRING) nome_regiao_administrativa,
    SAFE_CAST(pal AS STRING) projeto_loteamento, # boolean
    SAFE_CAST(f6 AS STRING) referencia, # verificar metadado
    SAFE_CAST(SAFE.PARSE_DATE('%d/%m/%Y', data) AS DATE) data_cadastro,  
    SAFE_CAST(legislacao AS STRING) legislacao,
    SAFE_CAST(pavimentos AS STRING) limite_pavimentos_permitido,
    SAFE_CAST(tx_ocupacao AS STRING) taxa_ocupacao,
    SAFE_CAST(REGEXP_REPLACE(iat, r',', '.') AS FLOAT64) AS indice_aproveitamento_terreno,
    SAFE_CAST(coef_adensamento AS STRING) coeficiente_adensamento,
    SAFE_CAST(afastamentos AS STRING) afastamento,
    SAFE_CAST(shape__area AS STRING) area,
    SAFE_CAST(shape__length AS STRING) comprimento,
    SAFE_CAST(geometry AS STRING) AS geometry_wkt,
    ST_GEOGFROMTEXT(geometry, make_valid => TRUE) AS geometry, 
FROM {{ source("brutos_dados_mestres_staging", "aeis") }}
