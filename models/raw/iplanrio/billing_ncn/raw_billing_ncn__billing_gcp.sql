{{
  config(
    alias="billing_gcp",
    materialized="table",
    description="Consolidação de dados de billing do GCP de múltiplas fontes (MAPS, Dados.Rio, IPLAN, IPLAN_OLD, SEI) com informações de controle de projetos e dimensão de data"
  )
}}

WITH
    gcp AS (

      -- MAPS
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,
      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      {{ source('billing_maps', 'gcp_billing_export_v1_01E886_BEEF88_126F35') }} AS t

      -- Dados.Rio
    UNION ALL
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,

      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      {{ source('billing_dados_rio', 'gcp_billing_export_resource_v1_017DB4_593D94_BEEC50') }} AS t
  UNION ALL
  -- IPLAN
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,
      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      {{ source('billing_iplan', 'gcp_billing_export_v1_01F105_3E298A_65D256') }} AS t
    UNION ALL

    -- IPLAN_OLD
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,
      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      {{ source('billing_iplan_old', 'gcp_billing_export_v1_01B087_0E43DD_2F1D01') }} AS t
  
    UNION ALL

    -- SEI 
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,

      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      {{ source('billing_iplan_sei', 'gcp_billing_export_v1_01E7FF_E3D5BA_3EF20C') }} AS t
      
     /* 
      UNION ALL

    -- SMTR 
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,

      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      `iplan-rio-smtr.billing_export_1036514_iplan_rio_smtr.gcp_billing_export_v1_010693_B1EC8D_C0D4DF` AS t

      UNION ALL

    -- SMS 
    SELECT
      CAST(t.usage_end_time AS date) AS usage_date,
      t.project.id AS project_id,
      t.project.number AS project_number,
      t.project.name AS project_name,
      t.project.labels AS project_labels,
      t.service.id AS service_id,
      t.service.description AS service_description,
      t.sku.id AS sku_id,
      t.sku.description AS sku_description,
      t.cost AS cost,

      (
      SELECT
        SUM(c.amount)
      FROM
        UNNEST(t.credits) AS c) AS total_credit
    FROM
      `  iplan-rio-sms.billing.gcp_billing_export_v1_01BAED_6E64B4_606041` AS t


   */
      ),

    dt as (select * from {{ source('billing_iplan', 'Date') }})

  SELECT
   *
  FROM
    dt left join GCP on dt.date = GCP.usage_date
  