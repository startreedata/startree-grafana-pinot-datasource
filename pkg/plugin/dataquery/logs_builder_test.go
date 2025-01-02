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
	client := test_helpers.SetupPinotAndCreateClient(t)

	driver := DriverFunc(func(ctx context.Context) backend.DataResponse {
		return ExecuteLogsBuilderQuery(ctx, client, LogsBuilderParams{
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
		})
	})

	got := driver(context.Background())

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
