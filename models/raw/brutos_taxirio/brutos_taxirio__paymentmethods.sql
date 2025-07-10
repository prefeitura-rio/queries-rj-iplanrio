SELECT
  SAFE_CAST (id as STRING) as id_pagamento_associado,
  SAFE_CAST (pindex as INT64) as pindex,
  SAFE_CAST (name as STRING) as nome,
  SAFE_CAST (type as STRING) as tipo,
FROM
  `rj-iplanrio.brutos_taxirio_staging.paymentmethods`
