{{
    config(
        materialized='view',
        schema='dashboard_gcp',
        alias='dim_service',
        tags=['dashboard', 'gcp', 'dimension', 'service'],
    )
}}

-- Dimensão de serviços GCP com categorização
-- Derivada de service.description do billing

WITH unique_services AS (
    SELECT DISTINCT service.description AS service_description
    FROM {{ ref('raw_gcp_billing') }}
    WHERE service.description IS NOT NULL
)

SELECT
    service_description,

    -- Categorização de serviços
    CASE
        -- Data Analytics & ML
        WHEN service_description IN ('BigQuery', 'BigQuery Reservation API', 'BigQuery Storage API')
            OR service_description LIKE '%BigQuery%'
        THEN 'Data Analytics'

        WHEN service_description IN ('Vertex AI', 'AI Platform', 'AutoML')
            OR service_description LIKE '%AI%'
            OR service_description LIKE '%ML%'
        THEN 'AI & ML'

        -- Compute
        WHEN service_description IN ('Compute Engine', 'App Engine', 'Cloud Run', 'Cloud Functions', 'Kubernetes Engine')
            OR service_description LIKE '%Compute%'
            OR service_description LIKE '%Engine%'
        THEN 'Compute'

        -- Storage
        WHEN service_description IN ('Cloud Storage', 'Persistent Disk', 'Filestore')
            OR service_description LIKE '%Storage%'
            OR service_description LIKE '%Disk%'
        THEN 'Storage'

        -- Database
        WHEN service_description IN ('Cloud SQL', 'Cloud Spanner', 'Cloud Bigtable', 'Firestore', 'Memorystore')
            OR service_description LIKE '%SQL%'
            OR service_description LIKE '%Database%'
            OR service_description LIKE '%Firestore%'
        THEN 'Database'

        -- Networking
        WHEN service_description IN ('Cloud Load Balancing', 'Cloud CDN', 'Cloud VPN', 'Cloud Interconnect', 'Cloud NAT')
            OR service_description LIKE '%Network%'
            OR service_description LIKE '%VPN%'
            OR service_description LIKE '%Load Balancing%'
        THEN 'Networking'

        -- Security
        WHEN service_description IN ('Cloud Armor', 'Cloud KMS', 'Secret Manager', 'Certificate Manager')
            OR service_description LIKE '%Security%'
            OR service_description LIKE '%KMS%'
        THEN 'Security'

        -- Monitoring & Logging
        WHEN service_description IN ('Cloud Logging', 'Cloud Monitoring', 'Cloud Trace', 'Error Reporting')
            OR service_description LIKE '%Logging%'
            OR service_description LIKE '%Monitoring%'
        THEN 'Observability'

        -- Outros
        ELSE 'Other'
    END AS service_category,

    -- Icon/Color mapping (para Looker Studio)
    CASE
        WHEN service_description LIKE '%BigQuery%' THEN '📊'
        WHEN service_description LIKE '%Compute%' THEN '⚙️'
        WHEN service_description LIKE '%Storage%' THEN '💾'
        WHEN service_description LIKE '%AI%' OR service_description LIKE '%ML%' THEN '🤖'
        WHEN service_description LIKE '%SQL%' OR service_description LIKE '%Database%' THEN '🗄️'
        WHEN service_description LIKE '%Network%' THEN '🌐'
        WHEN service_description LIKE '%Logging%' OR service_description LIKE '%Monitoring%' THEN '📈'
        ELSE '🔧'
    END AS service_icon

FROM unique_services
ORDER BY service_description
