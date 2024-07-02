package plugin

import (
	"testing"
)

const testQuery = `
SET useMultistageEngine=true;

WITH data AS (
  SELECT
    City AS "city",
    $__timeGroup("timestamp") AS "timestamp",
    Ride_Cancellations AS "cancellations"
  FROM
    $__table() 
  WHERE
    $__timeFilter("timestamp")
)

SELECT 
  r."city", 
  r."cancellations" / t."cancellations" * 100 AS $__metricAlias(),
  r."timestamp" AS $__timeAlias()
FROM (
  SELECT "city", "timestamp", sum("cancellations") AS "cancellations"
  FROM data 
  GROUP BY "city", "timestamp"
) r
JOIN (
  SELECT "timestamp", sum("cancellations") AS "cancellations"
  FROM data 
  GROUP BY "timestamp"
) t ON r."timestamp" = t."timestamp"
LIMIT 1000000
`

func TestExpandMacros(t *testing.T) {

}
