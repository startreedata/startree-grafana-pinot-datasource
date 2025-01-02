package dataquery

import (
	"context"
	"encoding/json"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestExtractTableDataFrame(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	resp, err := client.ExecuteSqlQuery(context.Background(),
		pinotlib.NewSqlQuery(`select __timestamp, __string, __long from allDataTypes limit 1`))
	require.NoError(t, err)
	require.True(t, resp.HasData())
	got := ExtractTableDataFrame(resp.ResultTable, "__timestamp")

	want := data.NewFrame("response",
		data.NewField("__timestamp", nil, []time.Time{time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC)}),
		data.NewField("__string", nil, []string{"row_0"}),
		data.NewField("__long", nil, []int64{0}),
	)
	assert.Equal(t, want, got)
}

func TestExtractLogsDataFrame(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	resp, err := client.ExecuteSqlQuery(context.Background(),
		pinotlib.NewSqlQuery(`select ts, message, ipAddr from nginxLogs limit 1`))
	require.NoError(t, err)
	require.True(t, resp.HasData())
	got, err := ExtractLogsDataFrame(resp.ResultTable, "ts", "message")

	want := data.NewFrame("response",
		data.NewField("labels", nil, []json.RawMessage{json.RawMessage(`{"ipAddr":"143.110.222.166"}`)}),
		data.NewField("Line", nil, []string{`143.110.222.166 - - [06/Nov/2024:00:02:05 +0000] "GET / HTTP/1.1" 403 134 "-" "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1" "-"`}),
		data.NewField("Time", nil, []time.Time{time.Date(2024, time.November, 6, 0, 2, 5, 0, time.UTC)}),
	)
	want.Meta = &data.FrameMeta{
		Custom: map[string]interface{}{"frameType": "LabeledTimeValues"},
	}
	assert.Equal(t, want, got)
}

func TestExtractColumnAsField(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	resp, err := client.ExecuteSqlQuery(context.Background(),
		pinotlib.NewSqlQuery(`select * from allDataTypes limit 3`))

	require.NoError(t, err)
	require.True(t, resp.HasData())

	testCases := []struct {
		column     string
		wantValues interface{}
	}{
		{column: "__double", wantValues: []float64{0, 0.1111111111111111, 0.2222222222222222}},
		{column: "__float", wantValues: []float32{0, 0.11111111, 0.22222222}},
		{column: "__int", wantValues: []int32{0, 111111, 222222}},
		{column: "__long", wantValues: []int64{0, 111111111111111, 222222222222222}},
		{column: "__string", wantValues: []string{"row_0", "row_1", "row_2"}},
		{column: "__bytes", wantValues: []string{"726f775f30", "726f775f31", "726f775f32"}},
		{column: "__bool", wantValues: []bool{true, false, true}},
		{column: "__big_decimal", wantValues: []string{"100000000000000000000", "100000000000000000001", "100000000000000000002"}},
		{column: "__json", wantValues: []json.RawMessage{
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`)}},
		{column: "__timestamp", wantValues: []time.Time{
			time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 1, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 2, 0, time.UTC)}},
		{column: "__map_string_long", wantValues: []json.RawMessage{
			json.RawMessage(`{"key1":1,"key2":2}`),
			json.RawMessage(`{"key1":1,"key2":2}`),
			json.RawMessage(`{"key1":1,"key2":2}`)}},
		{column: "__map_string_string", wantValues: []json.RawMessage{
			json.RawMessage(`{"key1":"val1","key2":"val2"}`),
			json.RawMessage(`{"key1":"val1","key2":"val2"}`),
			json.RawMessage(`{"key1":"val1","key2":"val2"}`)}},
	}

	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := pinotlib.GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			got := ExtractColumnAsField(resp.ResultTable, colIdx)
			require.Equal(t, data.NewField(tt.column, nil, tt.wantValues), got)
		})
	}
}
