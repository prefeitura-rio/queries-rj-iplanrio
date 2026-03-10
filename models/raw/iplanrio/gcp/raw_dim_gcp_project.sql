{{
    config(
        materialized='table',
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
    "rj-vision-ai",
    "rj-vision-ai-dev",
    "rj-iplanrio-dia",
    "rj-iplanrio-infra",
    "rj-iplanrio-terraform",
    "rj-teste-terraform",
    "rj-pic-dev",
    "tmp-subtd",
    "gen-lang-client-0287175976",
    "gen-lang-client-0035784078"
] %}

WITH all_projects AS (
    -- Projetos listados manualmente (INFORMATION_SCHEMA + projetos adicionais do IPLANRIO)
    SELECT DISTINCT project_id
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
            SELECT 'rj-vision-ai' UNION ALL
            -- ⬇️ NOVOS PROJETOS ADICIONADOS (2026-02-23):
            -- IPLANRIO
            SELECT 'rj-vision-ai-dev' UNION ALL
            SELECT 'rj-iplanrio-dia' UNION ALL
            SELECT 'rj-iplanrio-infra' UNION ALL
            SELECT 'rj-iplanrio-terraform' UNION ALL
            SELECT 'rj-teste-terraform' UNION ALL
            SELECT 'rj-pic-dev' UNION ALL
            SELECT 'tmp-subtd' UNION ALL
            SELECT 'gen-lang-client-0287175976' UNION ALL
            SELECT 'gen-lang-client-0035784078' UNION ALL
            -- SME
            SELECT 'rj-sme-danfe-ai' UNION ALL
            SELECT 'rj-sme-danfe-ai-staging' UNION ALL
            SELECT 'rj-sme-danfe-ai-dev' UNION ALL
            -- SMF (Fazenda)
            SELECT 'rj-nf-agent' UNION ALL
            -- Gabinete
            SELECT 'rj-gabinete' UNION ALL
            -- SMAS
            SELECT 'rj-smas-dev-432320' UNION ALL
            -- SMA
            SELECT 'rj-sma' UNION ALL
            -- SMS (Vigilância Sanitária)
            SELECT 'rj-ivisa' UNION ALL
            -- SSM
            SELECT 'rj-ssm' UNION ALL
            SELECT 'rj-ssm-dev' UNION ALL
            -- Externo/Hexagon (MAIOR CUSTO - $661k)
            SELECT 'softwareintegracao-hexagon'
    )
),

projects_with_classification AS (
    SELECT
        project_id,

        -- Derivação de orgao
        CASE
            -- Lista específica do IPLANRIO (reutilizando variável Jinja projetos_iplanrio)
            WHEN project_id IN (
                {% for p in projetos_iplanrio %}'{{ p }}'{% if not loop.last %}, {% endif %}{% endfor %}
            ) THEN 'IPLANRIO'

            -- Hackathon e similares
            WHEN REGEXP_CONTAINS(project_id, r'hackathon') THEN 'IPLANRIO'

            -- ⬇️ REGRAS ESPECIAIS PARA PROJETOS QUE NÃO SEGUEM PADRÃO rj-xxx

            -- SMF (Secretaria Municipal de Fazenda)
            WHEN project_id IN ('rj-nf-agent') THEN 'SMF'

            -- Gabinete do Prefeito
            WHEN project_id IN ('rj-gabinete') THEN 'GABINETE'

            -- SMA (Secretaria Municipal de Meio Ambiente)
            WHEN project_id IN ('rj-sma') THEN 'SMA'

            -- SMS - Vigilância Sanitária
            WHEN project_id IN ('rj-ivisa') THEN 'SMS'

            -- Hexagon (Software Integração) - MAIOR CUSTO ($661k)
            -- Órgão próprio para rastreamento de custos
            WHEN project_id = 'softwareintegracao-hexagon' THEN 'SOFTWAREINTEGRACAO-HEXAGON'

            -- RecRio
            WHEN REGEXP_CONTAINS(project_id, r'^rj-rec') THEN 'RECRIO'

            -- Padrão rj-xxx (extrai segundo componente como órgão)
            WHEN REGEXP_CONTAINS(project_id, r'^rj-') THEN
                UPPER(REGEXP_EXTRACT(project_id, r'^rj-([^-\s]+)'))

            -- Sem prefixo rj- (usa nome completo como órgão)
            WHEN project_id IS NOT NULL THEN UPPER(project_id)

            ELSE 'NÃO DEFINIDO'
        END AS orgao,

        -- Derivação de ambiente
        CASE
            -- Casos especiais com sufixo numérico
            WHEN project_id = 'rj-smas-dev-432320' THEN 'dev'
            -- Padrões normais
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
