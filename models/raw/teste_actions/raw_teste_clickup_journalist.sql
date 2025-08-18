-- tabela de CNO - Cadastro Nacional de Obras
{{
    config(
        materialized="table", 
    )
}}


SELECT
  "TESTE" as teste
    
FROM `rj-iplanrio.unidades_administrativas_staging.orgaos`