{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='dim_service',
        tags=['dashboard', 'gcp', 'dimension', 'service'],
    )
}}

-- Dimensão de serviços GCP com categorização
-- Lista todos os serviços únicos do billing com classificação por categoria

WITH unique_services AS (
    -- Extrair serviços únicos do billing
    SELECT DISTINCT service.description AS service_description
    FROM {{ ref('raw_gcp_billing') }}
    WHERE service.description IS NOT NULL
)

SELECT
    service_description,

    -- Categorização de serviços
    CASE
        -- Data Analytics
        WHEN service_description LIKE '%BigQuery%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Dataflow%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Pub/Sub%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Dataproc%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Data Fusion%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Composer%' THEN 'Data Analytics'

        -- Compute
        WHEN service_description LIKE '%Compute Engine%' THEN 'Compute'
        WHEN service_description LIKE '%Kubernetes Engine%' THEN 'Compute'
        WHEN service_description LIKE '%App Engine%' THEN 'Compute'
        WHEN service_description LIKE '%Cloud Run%' THEN 'Compute'
        WHEN service_description LIKE '%Cloud Functions%' THEN 'Compute'

        -- Storage
        WHEN service_description LIKE '%Cloud Storage%' THEN 'Storage'
        WHEN service_description LIKE '%Filestore%' THEN 'Storage'
        WHEN service_description LIKE '%Persistent Disk%' THEN 'Storage'

        -- Database
        WHEN service_description LIKE '%Cloud SQL%' THEN 'Database'
        WHEN service_description LIKE '%Firestore%' THEN 'Database'
        WHEN service_description LIKE '%Bigtable%' THEN 'Database'
        WHEN service_description LIKE '%Spanner%' THEN 'Database'
        WHEN service_description LIKE '%Memorystore%' THEN 'Database'

        -- Networking
        WHEN service_description LIKE '%VPC%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud CDN%' THEN 'Networking'
        WHEN service_description LIKE '%Load Balancing%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud DNS%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud NAT%' THEN 'Networking'
        WHEN service_description LIKE '%VPN%' THEN 'Networking'

        -- AI & ML
        WHEN service_description LIKE '%Vertex AI%' THEN 'AI & ML'
        WHEN service_description LIKE '%AI Platform%' THEN 'AI & ML'
        WHEN service_description LIKE '%Vision%' THEN 'AI & ML'
        WHEN service_description LIKE '%Natural Language%' THEN 'AI & ML'
        WHEN service_description LIKE '%Translation%' THEN 'AI & ML'

        -- Security
        WHEN service_description LIKE '%Cloud Armor%' THEN 'Security'
        WHEN service_description LIKE '%Secret Manager%' THEN 'Security'
        WHEN service_description LIKE '%Key Management%' THEN 'Security'
        WHEN service_description LIKE '%Security Command Center%' THEN 'Security'

        -- Observability
        WHEN service_description LIKE '%Cloud Logging%' THEN 'Observability'
        WHEN service_description LIKE '%Cloud Monitoring%' THEN 'Observability'
        WHEN service_description LIKE '%Cloud Trace%' THEN 'Observability'
        WHEN service_description LIKE '%Error Reporting%' THEN 'Observability'

        ELSE 'Other'
    END AS service_category,

    -- Ícone para visualização (emoji)
    CASE
        WHEN service_description LIKE '%BigQuery%' OR service_description LIKE '%Dataflow%' THEN '📊'
        WHEN service_description LIKE '%Compute Engine%' OR service_description LIKE '%Kubernetes%' THEN '💻'
        WHEN service_description LIKE '%Cloud Storage%' THEN '💾'
        WHEN service_description LIKE '%Cloud SQL%' OR service_description LIKE '%Firestore%' THEN '🗄️'
        WHEN service_description LIKE '%VPC%' OR service_description LIKE '%Load Balancing%' THEN '🌐'
        WHEN service_description LIKE '%Vertex AI%' THEN '🤖'
        WHEN service_description LIKE '%Cloud Armor%' OR service_description LIKE '%Secret Manager%' THEN '🔒'
        WHEN service_description LIKE '%Logging%' OR service_description LIKE '%Monitoring%' THEN '📈'
        ELSE '📦'
    END AS service_icon

FROM unique_services
ORDER BY service_category, service_description
