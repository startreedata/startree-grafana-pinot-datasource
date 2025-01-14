package dataquery

import (
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestExpandMacros(t *testing.T) {
	ctx := context.Background()

	engine := MacroEngine{
		TableName:   "CleanLogisticData",
		TimeAlias:   "time",
		MetricAlias: "metric",
		TableSchema: pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:        "timestamp",
				DataType:    "LONG",
				Format:      "1:SECONDS:EPOCH",
				Granularity: "30:SECONDS", // Unused
			}, {
				Name:     "timestamp_1m",
				DataType: "LONG",
				Format:   "1:MILLISECONDS:EPOCH",
			}},
		},
		TableConfigs: map[pinotlib.TableType]pinotlib.TableConfig{
			pinotlib.TableTypeRealTime: {
				TableName: "CleanLogisticData",
				TableType: pinotlib.TableTypeRealTime,
				IngestionConfig: pinotlib.IngestionConfig{
					TransformConfigs: []pinotlib.TransformConfig{
						{ColumnName: "timestamp_1m", TransformFunction: `DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
					},
				},
			},
		},
		TimeRange:    TimeRange{From: time.Unix(1, 0), To: time.Unix(90_001, 0)},
		IntervalSize: 1 * time.Hour,
	}

	testArgs := []struct {
		expr    string
		want    string
		wantErr string
	}{
		{expr: `$__table`, want: `"CleanLogisticData"`},
		{expr: `$__table()`, want: `"CleanLogisticData"`},
		{expr: `$__timeFilter`, wantErr: "failed to expand macro `timeFilter` (line 1, col 1): expected 1 required argument, got 0"},
		{expr: `$__timeFilter("timestamp")`, want: `"timestamp" >= 0 AND "timestamp" < 93600`},
		{expr: `$__timeFilter("timestamp", '1:HOURS')`, want: `"timestamp" >= 0 AND "timestamp" < 93600`},
		{expr: `$__timeFilter("timestamp", '1:MINUTES')`, want: `"timestamp" >= 0 AND "timestamp" < 90060`},
		{expr: `$__timeFilter("timestamp", '1:SECONDS')`, want: `"timestamp" >= 1 AND "timestamp" < 90001`},
		{expr: `$__timeGroup`, wantErr: "failed to expand macro `timeGroup` (line 1, col 1): expected 1 required argument, got 0"},
		{expr: `$__timeGroup("timestamp")`, want: `DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')`},
		{expr: `$__timeGroup("timestamp", '5:MINUTES')`, want: `DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`},
		{expr: `$__timeGroup("timestamp", '1:MINUTES')`, want: `"timestamp_1m"`},
		{expr: `$__timeTo`, wantErr: "failed to expand macro `timeTo` (line 1, col 1): expected 1 argument, got 0"},
		{expr: `$__timeTo("timestamp")`, want: `90001`},
		{expr: `$__timeFrom`, wantErr: "failed to expand macro `timeFrom` (line 1, col 1): expected 1 argument, got 0"},
		{expr: `$__timeFrom("timestamp")`, want: `1`},
		{expr: `$__timeAlias`, want: `"time"`},
		{expr: `$__metricAlias`, want: `"metric"`},
		{expr: `$__timeFilterMillis`, wantErr: "failed to expand macro `timeFilterMillis` (line 1, col 1): expected 1 required argument, got 0"},
		{expr: `$__timeFilterMillis("timestamp")`, want: `"timestamp" >= 0 AND "timestamp" < 93600000`},
		{expr: `$__timeFilterMillis("timestamp", '1:MINUTES')`, want: `"timestamp" >= 0 AND "timestamp" < 90060000`},
		{expr: `$__timeToMillis`, want: `90001000`},
		{expr: `$__timeFromMillis`, want: `1000`},
		{expr: `$__granularityMillis`, want: `3600000`},
		{expr: `$__granularityMillis('1:MINUTES')`, want: `60000`},
		{expr: `$__panelMillis`, want: `90000000`},
		{expr: `not a macro`, want: `not a macro`},
	}

	for _, tt := range testArgs {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := engine.ExpandMacros(ctx, tt.expr)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Equal(t, "", got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestMacroExprFor(t *testing.T) {
	ctx := context.Background()

	engine := MacroEngine{
		TableName:   "CleanLogisticData",
		TimeAlias:   "time",
		MetricAlias: "metric",
		TableSchema: pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:        "timestamp",
				DataType:    "LONG",
				Format:      "1:SECONDS:EPOCH",
				Granularity: "30:SECONDS", // Unused
			}},
		},
		TimeRange:    TimeRange{From: time.Unix(1, 0), To: time.Unix(86401, 0)},
		IntervalSize: 1 * time.Hour,
	}

	t.Run(MacroTable, func(t *testing.T) {
		gotExpr := MacroExprFor(MacroTable)
		assert.Equal(t, "$__table()", gotExpr)
		res, err := engine.ExpandTableName(ctx, gotExpr)
		assert.NoError(t, err)
		assert.Equal(t, `"CleanLogisticData"`, strings.TrimSpace(res))
	})

	t.Run(MacroTimeGroup, func(t *testing.T) {
		gotExpr := MacroExprFor(MacroTimeGroup, `"timestamp"`, "'1:MINUTES'")
		assert.Equal(t, `$__timeGroup("timestamp", '1:MINUTES')`, gotExpr)
		res, err := engine.ExpandTimeGroup(ctx, gotExpr)
		assert.NoError(t, err)
		assert.Equal(t,
			`DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
			strings.TrimSpace(res))
	})
}

func TestExpandMacrosExampleQuery(t *testing.T) {
	ctx := context.Background()

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
     "timestamp" >= 1523714400 AND "timestamp" < 1512241200 
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
		TableSchema: pinotlib.TableSchema{
			DateTimeFieldSpecs: []pinotlib.DateTimeFieldSpec{{
				Name:        "timestamp",
				DataType:    "LONG",
				Format:      "1:SECONDS:EPOCH",
				Granularity: "1:SECONDS",
			}},
		},
		TimeRange:    TimeRange{To: time.Unix(1512238653, 0), From: time.Unix(1523714551, 0)},
		IntervalSize: 1 * time.Hour,
	}

	got, err := engine.ExpandMacros(ctx, `
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
