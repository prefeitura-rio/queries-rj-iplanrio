{{
    config(
        materialized='view',
        alias='dim_service',
        tags=['dashboard', 'gcp', 'dimension', 'service'],
    )
}}

WITH unique_services AS (
    SELECT DISTINCT service.description AS service_description
    FROM {{ ref('raw_gcp_billing') }}
    WHERE service.description IS NOT NULL
)

SELECT
    service_description,
    CASE
        WHEN service_description LIKE '%BigQuery%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Dataflow%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Pub/Sub%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Dataproc%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Data Fusion%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Composer%' THEN 'Data Analytics'
        WHEN service_description LIKE '%Compute Engine%' THEN 'Compute'
        WHEN service_description LIKE '%Kubernetes Engine%' THEN 'Compute'
        WHEN service_description LIKE '%App Engine%' THEN 'Compute'
        WHEN service_description LIKE '%Cloud Run%' THEN 'Compute'
        WHEN service_description LIKE '%Cloud Functions%' THEN 'Compute'
        WHEN service_description LIKE '%Cloud Storage%' THEN 'Storage'
        WHEN service_description LIKE '%Filestore%' THEN 'Storage'
        WHEN service_description LIKE '%Persistent Disk%' THEN 'Storage'
        WHEN service_description LIKE '%Cloud SQL%' THEN 'Database'
        WHEN service_description LIKE '%Firestore%' THEN 'Database'
        WHEN service_description LIKE '%Bigtable%' THEN 'Database'
        WHEN service_description LIKE '%Spanner%' THEN 'Database'
        WHEN service_description LIKE '%Memorystore%' THEN 'Database'
        WHEN service_description LIKE '%VPC%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud CDN%' THEN 'Networking'
        WHEN service_description LIKE '%Load Balancing%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud DNS%' THEN 'Networking'
        WHEN service_description LIKE '%Cloud NAT%' THEN 'Networking'
        WHEN service_description LIKE '%VPN%' THEN 'Networking'
        WHEN service_description LIKE '%Vertex AI%' THEN 'AI & ML'
        WHEN service_description LIKE '%AI Platform%' THEN 'AI & ML'
        WHEN service_description LIKE '%Vision%' THEN 'AI & ML'
        WHEN service_description LIKE '%Natural Language%' THEN 'AI & ML'
        WHEN service_description LIKE '%Translation%' THEN 'AI & ML'
        WHEN service_description LIKE '%Cloud Armor%' THEN 'Security'
        WHEN service_description LIKE '%Secret Manager%' THEN 'Security'
        WHEN service_description LIKE '%Key Management%' THEN 'Security'
        WHEN service_description LIKE '%Security Command Center%' THEN 'Security'
        WHEN service_description LIKE '%Cloud Logging%' THEN 'Observability'
        WHEN service_description LIKE '%Cloud Monitoring%' THEN 'Observability'
        WHEN service_description LIKE '%Cloud Trace%' THEN 'Observability'
        WHEN service_description LIKE '%Error Reporting%' THEN 'Observability'

        ELSE 'Other'
    END AS service_category,
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
