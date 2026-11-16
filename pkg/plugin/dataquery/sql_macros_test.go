package dataquery

import (
	"context"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
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
		TableSchema: pinot.TableSchema{
			DateTimeFieldSpecs: []pinot.DateTimeFieldSpec{{
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
		TableConfigs: map[pinot.TableType]pinot.TableConfig{
			pinot.TableTypeRealTime: {
				TableName: "CleanLogisticData",
				TableType: pinot.TableTypeRealTime,
				IngestionConfig: pinot.IngestionConfig{
					TransformConfigs: []pinot.TransformConfig{
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
		{expr: `$__timeFilter`, wantErr: "failed to expand macro `timeFilter` (line 1, col 1): the table has several time columns (timestamp, timestamp_1m) and no segmentsConfig.timeColumnName; pass the column explicitly, e.g. $__timeFilter(\"timestamp\")"},
		{expr: `$__timeFilter("timestamp")`, want: `"timestamp" >= 0 AND "timestamp" < 93600`},
		{expr: `$__timeFilter("timestamp", '1:HOURS')`, want: `"timestamp" >= 0 AND "timestamp" < 93600`},
		{expr: `$__timeFilter("timestamp", '1:MINUTES')`, want: `"timestamp" >= 0 AND "timestamp" < 90060`},
		{expr: `$__timeFilter("timestamp", '1:SECONDS')`, want: `"timestamp" >= 1 AND "timestamp" < 90001`},
		{expr: `$__timeGroup`, wantErr: "failed to expand macro `timeGroup` (line 1, col 1): the table has several time columns (timestamp, timestamp_1m) and no segmentsConfig.timeColumnName; pass the column explicitly, e.g. $__timeFilter(\"timestamp\")"},
		{expr: `$__timeGroup("timestamp")`, want: `DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')`},
		{expr: `$__timeGroup("timestamp", '5:MINUTES')`, want: `DATETIMECONVERT("timestamp", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`},
		{expr: `$__timeGroup("timestamp", '1:MINUTES')`, want: `"timestamp_1m"`},
		{expr: `$__timeTo`, wantErr: "failed to expand macro `timeTo` (line 1, col 1): the table has several time columns (timestamp, timestamp_1m) and no segmentsConfig.timeColumnName; pass the column explicitly, e.g. $__timeFilter(\"timestamp\")"},
		{expr: `$__timeTo("timestamp")`, want: `90001`},
		{expr: `$__timeFrom`, wantErr: "failed to expand macro `timeFrom` (line 1, col 1): the table has several time columns (timestamp, timestamp_1m) and no segmentsConfig.timeColumnName; pass the column explicitly, e.g. $__timeFilter(\"timestamp\")"},
		{expr: `$__timeFrom("timestamp")`, want: `1`},
		{expr: `$__timeAlias`, want: `"time"`},
		{expr: `$__metricAlias`, want: `"metric"`},
		{expr: `$__timeFilterMillis`, wantErr: "failed to expand macro `timeFilterMillis` (line 1, col 1): the table has several time columns (timestamp, timestamp_1m) and no segmentsConfig.timeColumnName; pass the column explicitly, e.g. $__timeFilter(\"timestamp\")"},
		{expr: `$__timeFilterMillis("timestamp")`, want: `"timestamp" >= 0 AND "timestamp" < 93600000`},
		{expr: `$__timeFilterMillis("timestamp", '1:MINUTES')`, want: `"timestamp" >= 0 AND "timestamp" < 90060000`},
		{expr: `$__timeToMillis`, want: `90001000`},
		{expr: `$__timeFromMillis`, want: `1000`},
		{expr: `$__granularityMillis`, want: `3600000`},
		{expr: `$__granularityMillis('1:MINUTES')`, want: `60000`},
		{expr: `$__panelMillis`, want: `90000000`},
		{expr: `$__fromTime`, want: `1`},
		{expr: `$__toTime`, want: `90001`},
		{expr: `$__interval_s`, want: `3600`},
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

func TestExpandAdHocFilter(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		filters []pinot.AdHocFilter
		expr    string
		want    string
	}{
		{
			name:    "no filters expands to no-op",
			filters: nil,
			expr:    `WHERE $__adHocFilter`,
			want:    `WHERE  TRUE`,
		},
		{
			name:    "single filter",
			filters: []pinot.AdHocFilter{{Key: "city", Operator: "=", Value: "NY"}},
			expr:    `$__adHocFilter`,
			want:    `"city" = 'NY'`,
		},
		{
			name: "multiple filters are AND-joined",
			filters: []pinot.AdHocFilter{
				{Key: "city", Operator: "=", Value: "NY"},
				{Key: "age", Operator: ">", Value: "30"},
			},
			expr: `$__adHocFilter`,
			want: `"city" = 'NY' AND "age" > '30'`,
		},
		{
			name: "every supported operator",
			filters: []pinot.AdHocFilter{
				{Key: "a", Operator: "=", Value: "1"},
				{Key: "b", Operator: "!=", Value: "2"},
				{Key: "c", Operator: "<", Value: "3"},
				{Key: "d", Operator: ">", Value: "4"},
				{Key: "e", Operator: "=~", Value: "x.*"},
				{Key: "f", Operator: "!~", Value: "y.*"},
			},
			expr: `$__adHocFilter`,
			want: `"a" = '1' AND "b" != '2' AND "c" < '3' AND "d" > '4' AND REGEXP_LIKE("e", 'x.*') AND NOT REGEXP_LIKE("f", 'y.*')`,
		},
		{
			name:    "value is escaped against injection",
			filters: []pinot.AdHocFilter{{Key: "city", Operator: "=", Value: "x' OR '1'='1"}},
			expr:    `$__adHocFilter`,
			want:    `"city" = 'x'' OR ''1''=''1'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := MacroEngine{AdHocFilters: tt.filters}
			got, err := engine.ExpandMacros(ctx, tt.expr)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMacroExprFor(t *testing.T) {
	ctx := context.Background()

	engine := MacroEngine{
		TableName:   "CleanLogisticData",
		TimeAlias:   "time",
		MetricAlias: "metric",
		TableSchema: pinot.TableSchema{
			DateTimeFieldSpecs: []pinot.DateTimeFieldSpec{{
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
		gotExpr := MacroExprFor(MacroTable).String()
		assert.Equal(t, "$__table()", gotExpr)
		res, err := engine.ExpandTableName(ctx, gotExpr)
		assert.NoError(t, err)
		assert.Equal(t, `"CleanLogisticData"`, strings.TrimSpace(res))
	})

	t.Run(MacroTimeGroup, func(t *testing.T) {
		gotExpr := MacroExprFor(MacroTimeGroup, `"timestamp"`, "'1:MINUTES'").String()
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
		TableSchema: pinot.TableSchema{
			DateTimeFieldSpecs: []pinot.DateTimeFieldSpec{{
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

func TestOffsetCoords(t *testing.T) {
	line, col := offsetCoords(`
    hello  `, 5)
	assert.Equal(t, 2, line)
	assert.Equal(t, 5, col)
}

// DP-3035: time macros must apply the dashboard time range correctly for the column formats users store.
func TestExpandMacros_TimeColumnResolution(t *testing.T) {
	ctx := context.Background()
	timeRange := TimeRange{From: time.UnixMilli(1_699_999_980_000), To: time.UnixMilli(1_700_003_580_000)}

	schema := pinot.TableSchema{
		DimensionFieldSpecs: []pinot.DimensionFieldSpec{
			{Name: "pt", DataType: "STRING"},
			{Name: "ts_sec", DataType: "LONG"},
			{Name: "ts_type", DataType: "TIMESTAMP"},
		},
		MetricFieldSpecs: []pinot.MetricFieldSpec{{Name: "attempt", DataType: "LONG"}},
		DateTimeFieldSpecs: []pinot.DateTimeFieldSpec{
			{Name: "timestamp", DataType: "LONG", Format: "1:MILLISECONDS:EPOCH", Granularity: "1:MINUTES"},
			{Name: "day_str", DataType: "STRING", Format: "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", Granularity: "1:DAYS"},
		},
	}
	configs := pinot.ListTableConfigsResponse{
		pinot.TableTypeOffline: {TableName: "events", SegmentsConfig: pinot.SegmentsConfig{TimeColumnName: "timestamp"}},
	}

	tests := []struct {
		name         string
		expr         string
		want         string
		wantErr      string
		wantWarnings int
	}{
		{name: "no argument uses segmentsConfig.timeColumnName", expr: `$__timeFilter`,
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000`},
		{name: "empty parens use the default column", expr: `$__timeFilter()`,
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000`},
		{name: "timeFrom/timeTo without argument", expr: `$__timeFrom AND $__timeTo()`,
			want: `1699999980000  AND  1700003580000`},
		{name: "nested parentheses in expression argument",
			expr: `$__timeFilter(DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES'))`,
			want: `DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES') >= 1699999980000 AND ` +
				`DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES') < 1700003580000`},
		{name: "expression with granularity argument",
			expr: `$__timeFilter(DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES'), '1:MINUTES')`,
			want: `DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES') >= 1699999980000 AND ` +
				`DATETIMECONVERT("timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES') < 1700003580000`},
		{name: "single-quoted column name", expr: `$__timeFilter('timestamp')`,
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000`},
		{name: "backtick-quoted column name", expr: "$__timeFilter(`timestamp`)",
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000`},
		{name: "qualified column name", expr: `$__timeFilter(r.timestamp)`,
			want: `r.timestamp >= 1699999980000 AND r.timestamp < 1700003580000`},
		{name: "numeric non-datetime column warns instead of silently assuming millis", expr: `$__timeFilter(ts_sec)`,
			want: `"ts_sec" >= 1699999980000 AND "ts_sec" < 1700003580000`, wantWarnings: 1},
		{name: "TIMESTAMP column is millis without warning", expr: `$__timeFilter(ts_type)`,
			want: `"ts_type" >= 1699999980000 AND "ts_type" < 1700003580000`},
		{name: "string column is an error", expr: `$__timeFilter(pt)`,
			wantErr: "failed to expand macro `timeFilter` (line 1, col 1): column `pt` has type STRING and is not a date time column; time macros need a date time column or a numeric epoch column"},
		{name: "unknown column is an error", expr: `$__timeFilter("nope")`,
			wantErr: "failed to expand macro `timeFilter` (line 1, col 1): column `nope` not found in table `events`"},
		{name: "unsupported date time format is an error", expr: `$__timeFilter(day_str)`,
			wantErr: "failed to expand macro `timeFilter` (line 1, col 1): column `day_str`: failed to parse date time format `1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd`: simple date time not supported"},
		{name: "millis macro is not swallowed by timeFilter", expr: `$__timeFilterMillis("timestamp", '1:MINUTES')`,
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000`},
		{name: "unclosed parenthesis", expr: `$__timeFilter("timestamp"`,
			wantErr: "failed to expand macro `timeFilter` (line 1, col 1): missing closing parenthesis"},
		{name: "parenthesis inside a string literal", expr: `$__timeFilter("timestamp", '1:MINUTES') AND pt = ')'`,
			want: `"timestamp" >= 1699999980000 AND "timestamp" < 1700003580000  AND pt = ')'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var warnings []string
			engine := MacroEngine{TableName: "events", TableSchema: schema, TableConfigs: configs,
				TimeRange: timeRange, IntervalSize: time.Minute, Warnings: &warnings}
			got, err := engine.ExpandMacros(ctx, tt.expr)
			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Len(t, warnings, tt.wantWarnings)
		})
	}
}

func TestExpandMacros_LegacyTimeFieldSpec(t *testing.T) {
	engine := MacroEngine{
		TableName: "legacy",
		TableSchema: pinot.TableSchema{TimeFieldSpec: &pinot.TimeFieldSpec{
			IncomingGranularitySpec: &pinot.TimeGranularitySpec{Name: "daysSinceEpoch", DataType: "INT", TimeType: "DAYS", TimeUnitSize: 1},
		}},
		TimeRange:    TimeRange{From: time.Unix(86400*10, 0), To: time.Unix(86400*20, 0)},
		IntervalSize: 24 * time.Hour,
	}
	got, err := engine.ExpandMacros(context.Background(), `$__timeFilter`)
	assert.NoError(t, err)
	assert.Equal(t, `"daysSinceEpoch" >= 10 AND "daysSinceEpoch" < 20`, got)
}
