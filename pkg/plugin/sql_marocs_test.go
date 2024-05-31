package plugin

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestExpandMacros(t *testing.T) {
	want := strings.TrimSpace(`
SET useMultistageEngine=true;

WITH data AS (
  SELECT
    City AS "city",
     DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')  AS "timestamp",
    Ride_Cancellations AS "cancellations"
  FROM
     "CleanLogisticData" 
  WHERE
     "timestamp" >= 1523714551 AND "timestamp" <= 1512238653 
)

SELECT 
  r."city", 
  r."cancellations" / t."cancellations" * 100 AS  "metric" ,
  r."timestamp" AS  "time" 
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
`)

	engine := MacroEngine{
		TableName:   "CleanLogisticData",
		TimeAlias:   "time",
		MetricAlias: "metric",
		TableSchema: TableSchema{
			DateTimeFieldSpecs: []DateTimeFieldSpec{{
				Name:        "timestamp",
				DataType:    "LONG",
				Format:      "1:SECONDS:EPOCH",
				Granularity: "1:SECONDS",
			}},
		},
		TimeRange:    TimeRange{To: time.Unix(1512238653, 0), From: time.Unix(1523714551, 0)},
		IntervalSize: 1 * time.Hour,
	}

	got, err := engine.ExpandMacros(`
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
`)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestInvocationCoords(t *testing.T) {
	line, col := invocationCoords(`
    hello  `, "hello")
	assert.Equal(t, 2, line)
	assert.Equal(t, 5, col)
}
