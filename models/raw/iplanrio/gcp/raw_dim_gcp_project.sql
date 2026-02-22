{{
    config(
        materialized='table',
        schema='brutos_gcp',
        alias='dim_gcp_project'
    )
}}

-- Dimensão de projetos GCP com classificação de orgao/ambiente
-- Centraliza lógica de derivação para evitar duplicação em modelos downstream

{% set projetos_iplanrio = [
    "dados-rio-billing",
    "datario",
    "datario-dev",
    "hackathon-fgv-03-2024",
    "rj-caio",
    "rj-chatbot",
    "rj-chatbot-dev",
    "rj-crm-registry",
    "rj-crm-registry-dev",
    "rj-comunicacao",
    "rj-comunicacao-dev",
    "rj-datalab-sandbox",
    "rj-escritorio",
    "rj-escritorio-dev",
    "rj-ia-desenvolvimento",
    "rj-mapa-realizacoes",
    "rj-mapa-realizacoes-dev",
    "rj-precipitacao",
    "rj-superapp",
    "rj-superapp-staging",
    "rj-vision-ai"
] %}

WITH all_projects AS (
    -- Projetos da lista explícita de INFORMATION_SCHEMA.JOBS_BY_PROJECT
    SELECT DISTINCT '{{ projetos | join("' AS project_id UNION ALL SELECT '") }}' AS project_id
    FROM (
        SELECT
            'rj-cetrio' AS project_id UNION ALL SELECT 'rj-cetrio-dev' UNION ALL
            SELECT 'rj-chatbot' UNION ALL SELECT 'rj-chatbot-dev' UNION ALL
            SELECT 'rj-civitas' UNION ALL SELECT 'rj-civitas-dev' UNION ALL
            SELECT 'rj-cmp' UNION ALL
            SELECT 'rj-comunicacao' UNION ALL SELECT 'rj-comunicacao-dev' UNION ALL
            SELECT 'rj-cor' UNION ALL SELECT 'rj-cor-dev' UNION ALL
            SELECT 'rj-crm-registry' UNION ALL SELECT 'rj-crm-registry-dev' UNION ALL
            SELECT 'rj-cvl' UNION ALL SELECT 'rj-cvl-dev' UNION ALL
            SELECT 'rj-datalab-sandbox' UNION ALL
            SELECT 'rj-ia-desenvolvimento' UNION ALL
            SELECT 'rj-escritorio' UNION ALL SELECT 'rj-escritorio-dev' UNION ALL
            SELECT 'rj-iplanrio' UNION ALL SELECT 'rj-iplanrio-dev' UNION ALL SELECT 'rj-iplanrio-ia-dev' UNION ALL
            SELECT 'rj-ipp' UNION ALL SELECT 'rj-ipp-dev' UNION ALL
            SELECT 'rj-mapa-realizacoes' UNION ALL SELECT 'rj-mapa-realizacoes-dev' UNION ALL
            SELECT 'rj-multirio' UNION ALL SELECT 'rj-multirio-dev' UNION ALL
            SELECT 'rj-pgm' UNION ALL
            SELECT 'rj-precipitacao' UNION ALL
            SELECT 'rj-rec-rio' UNION ALL SELECT 'rj-rec-rio-dev' UNION ALL
            SELECT 'rj-rioaguas' UNION ALL SELECT 'rj-rioaguas-dev' UNION ALL
            SELECT 'rj-riosaude' UNION ALL
            SELECT 'rj-seconserva' UNION ALL SELECT 'rj-seconserva-dev' UNION ALL
            SELECT 'rj-segovi' UNION ALL SELECT 'rj-segovi-dev' UNION ALL
            SELECT 'rj-seop' UNION ALL SELECT 'rj-seop-dev' UNION ALL
            SELECT 'rj-setur' UNION ALL SELECT 'rj-setur-dev' UNION ALL
            SELECT 'rj-siurb' UNION ALL
            SELECT 'rj-smac' UNION ALL SELECT 'rj-smac-dev' UNION ALL
            SELECT 'rj-smas' UNION ALL SELECT 'rj-smas-dev' UNION ALL
            SELECT 'rj-smdue' UNION ALL
            SELECT 'rj-sme' UNION ALL SELECT 'rj-sme-dev' UNION ALL
            SELECT 'rj-smfp' UNION ALL SELECT 'rj-smfp-dev' UNION ALL SELECT 'rj-smfp-egp' UNION ALL
            SELECT 'rj-smi' UNION ALL SELECT 'rj-smi-dev' UNION ALL
            SELECT 'rj-sms' UNION ALL SELECT 'rj-sms-dev' UNION ALL SELECT 'rj-sms-sandbox' UNION ALL
            SELECT 'rj-smtr' UNION ALL SELECT 'rj-smtr-dev' UNION ALL SELECT 'rj-smtr-staging' UNION ALL
            -- Projetos adicionais do IPLANRIO (não estão na lista de jobs)
            SELECT 'dados-rio-billing' UNION ALL
            SELECT 'datario' UNION ALL SELECT 'datario-dev' UNION ALL
            SELECT 'hackathon-fgv-03-2024' UNION ALL
            SELECT 'rj-caio' UNION ALL
            SELECT 'rj-superapp' UNION ALL SELECT 'rj-superapp-staging' UNION ALL
            SELECT 'rj-vision-ai'
    )
),

projects_with_classification AS (
    SELECT
        project_id,

        -- Derivação de orgao
        CASE
            -- Lista específica do IPLANRIO (baseado em mart_gerenciamento_custo_gcp.sql)
            WHEN project_id IN (
                'dados-rio-billing', 'datario', 'datario-dev',
                'hackathon-fgv-03-2024',
                'rj-caio',
                'rj-chatbot', 'rj-chatbot-dev',
                'rj-comunicacao', 'rj-comunicacao-dev',
                'rj-crm-registry', 'rj-crm-registry-dev',
                'rj-datalab-sandbox',
                'rj-escritorio', 'rj-escritorio-dev',
                'rj-ia-desenvolvimento',
                'rj-mapa-realizacoes', 'rj-mapa-realizacoes-dev',
                'rj-precipitacao',
                'rj-superapp', 'rj-superapp-staging',
                'rj-vision-ai'
            ) THEN 'IPLANRIO'

            -- Hackathon e similares
            WHEN REGEXP_CONTAINS(project_id, r'hackathon') THEN 'IPLANRIO'

            -- RecRio
            WHEN REGEXP_CONTAINS(project_id, r'^rj-rec') THEN 'RECRIO'

            -- Padrão rj-xxx
            WHEN REGEXP_CONTAINS(project_id, r'^rj-') THEN
                UPPER(REGEXP_EXTRACT(project_id, r'^rj-([^-\s]+)'))

            -- Sem prefixo rj-
            WHEN project_id IS NOT NULL THEN UPPER(project_id)

            ELSE 'NÃO DEFINIDO'
        END AS orgao,

        -- Derivação de ambiente
        CASE
            WHEN REGEXP_CONTAINS(project_id, r'-dev$') THEN 'dev'
            WHEN REGEXP_CONTAINS(project_id, r'-sandbox$') THEN 'sandbox'
            WHEN REGEXP_CONTAINS(project_id, r'-staging$') THEN 'dev'
            ELSE 'prod'
        END AS ambiente,

        -- projeto_base: remove sufixos de ambiente
        REGEXP_REPLACE(project_id, r'-(dev|staging|sandbox)$', '') AS projeto_base

    FROM all_projects
)

SELECT
    project_id,
    orgao,
    ambiente,
    projeto_base
FROM projects_with_classification
ORDER BY project_id
