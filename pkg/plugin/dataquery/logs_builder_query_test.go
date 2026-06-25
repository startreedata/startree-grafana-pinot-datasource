package dataquery

import (
	"context"
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExecuteLogsBuilderQuery(t *testing.T) {
	ctx := context.Background()
	client := test_helpers.SetupPinotAndCreateClient(t)

	got := LogsBuilderQuery{
		TimeRange: TimeRange{
			From: time.Date(2024, 06, 01, 00, 00, 00, 0, time.UTC),
			To:   time.Date(2024, 12, 31, 00, 00, 00, 0, time.UTC),
		},
		TableName:       "nginxLogs",
		TimeColumn:      "ts",
		LogColumn:       ComplexField{Name: "message"},
		MetadataColumns: []ComplexField{{Name: "ipAddr"}},
		RegexpExtractors: []RegexpExtractor{{
			Source:  ComplexField{Name: "message"},
			Pattern: "GET .* (HTTP/\\d\\.\\d)",
			Group:   1,
			Alias:   "httpVer",
		}},
		Limit: 1,
	}.Execute(client, ctx)

	assert.Equal(t, backend.Status(200), got.Status)
	assert.NotEmpty(t, got.Frames)

	wantFrame := data.NewFrame("response",
		data.NewField("labels", nil, []json.RawMessage{json.RawMessage(`{"httpVer":"HTTP/1.1","ipAddr":"143.110.222.166"}`)}),
		data.NewField("Line", nil, []string{`143.110.222.166 - - [06/Nov/2024:00:02:05 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"`}),
		data.NewField("Time", nil, []time.Time{time.Date(2024, time.November, 6, 0, 2, 5, 0, time.UTC)}),
	)
	wantFrame.Meta = &data.FrameMeta{
		Custom: map[string]interface{}{"frameType": "LabeledTimeValues"},
	}

	assert.Equal(t, wantFrame, got.Frames[0])
}

func TestLogsBuilderQueryLevelColumn(t *testing.T) {
	// LevelColumn must render as a column aliased "level" so Grafana colors log rows by level.
	sql, err := LogsBuilderQuery{
		TableName:   "nginxLogs",
		TimeColumn:  "ts",
		LogColumn:   ComplexField{Name: "message"},
		LevelColumn: ComplexField{Name: "logLevel"},
	}.RenderSqlWithMacros()

	assert.NoError(t, err)
	assert.Contains(t, sql, `"logLevel" AS 'level'`)
}

func TestLogsBuilderQueryVolume(t *testing.T) {
	// The logs-volume supplementary query is a bucketed count(*) over the same table/filters,
	// broken down by the level column when one is set.
	sql, err := LogsBuilderQuery{
		TableName:        "nginxLogs",
		TimeColumn:       "ts",
		LogColumn:        ComplexField{Name: "message"},
		LevelColumn:      ComplexField{Name: "logLevel"},
		DimensionFilters: []DimensionFilter{{ColumnName: "method", Operator: "=", ValueExprs: []string{"'GET'"}}},
	}.VolumeQuery().RenderSqlWithMacros()

	assert.NoError(t, err)
	assert.Contains(t, sql, `COUNT("*")`)
	assert.Contains(t, sql, `$__timeGroup("ts", 'auto')`)
	assert.Contains(t, sql, `"logLevel"`) // broken down by level
	assert.Contains(t, sql, `"method" = 'GET'`)
}

func TestLogsBuilderQueryVolumeNoLevel(t *testing.T) {
	// Without a level column the volume is a single count(*) series (no GROUP BY dimension).
	sql, err := LogsBuilderQuery{
		TableName:  "nginxLogs",
		TimeColumn: "ts",
		LogColumn:  ComplexField{Name: "message"},
	}.VolumeQuery().RenderSqlWithMacros()

	assert.NoError(t, err)
	assert.Contains(t, sql, `COUNT("*")`)
	assert.Contains(t, sql, `$__timeGroup("ts", 'auto')`)
}

func TestLogsBuilderQuerySortDirection(t *testing.T) {
	// The backward leg of log-row context fetches the rows immediately before the anchor, so the
	// logs query must sort newest-first.
	sql, err := LogsBuilderQuery{
		TableName:     "nginxLogs",
		TimeColumn:    "ts",
		LogColumn:     ComplexField{Name: "message"},
		SortDirection: "DESC",
	}.RenderSqlWithMacros()

	assert.NoError(t, err)
	assert.Contains(t, sql, `"ts" DESC`)
	assert.Contains(t, sql, `"__message" DESC`)
}
