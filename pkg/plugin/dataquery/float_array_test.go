package dataquery

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/require"
)

func TestFloatArrayTableFrameRoundTrip(t *testing.T) {
	// Decode a broker response to exercise the same value types as live queries.
	const response = `{"dataSchema":{"columnNames":["sensor","readings"],"columnDataTypes":["STRING","FLOAT_ARRAY"]},"rows":[["a",[1.5,-2.25,0]],["special",["NaN","Infinity","-Infinity"]],["empty",[]],["missing",null]]}`
	var result pinot.ResultTable
	decoder := json.NewDecoder(strings.NewReader(response))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&result))
	frame, err := ExtractTableDataFrame(&result, "")
	require.NoError(t, err)
	require.Len(t, frame.Fields, 2)
	require.Equal(t, "a", frame.Fields[0].At(0))
	want := []string{`[1.5,-2.25,0]`, `["NaN","Infinity","-Infinity"]`, `[]`, `null`}
	for i, value := range want {
		require.JSONEq(t, value, string(frame.Fields[1].At(i).(json.RawMessage)))
	}
	// Grafana transports backend frames through Arrow, not plain Go values.
	encoded, err := frame.MarshalArrow()
	require.NoError(t, err)
	restored, err := data.UnmarshalArrowFrame(encoded)
	require.NoError(t, err)
	for i, value := range want {
		require.JSONEq(t, value, string(restored.Fields[1].At(i).(json.RawMessage)))
	}
	jsonFrame, err := json.Marshal(restored)
	require.NoError(t, err)
	require.Contains(t, string(jsonFrame), `"readings"`)
}

func TestFloatArrayCodeQuery(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)
	got := PinotQlCodeQuery{
		TableName:    "arrayDataTypes",
		DisplayType:  DisplayTypeTable,
		IntervalSize: time.Minute,
		Code:         `SELECT "__float_array" FROM $__table() ORDER BY "__timestamp" ASC`,
	}.Execute(client, context.Background())
	require.NoError(t, got.Error)
	require.Equal(t, backend.StatusOK, got.Status)
	require.Len(t, got.Frames, 1)
	require.Equal(t, data.NewField("__float_array", nil, []json.RawMessage{
		json.RawMessage(`[1.5,-2.25,0.0]`),
		json.RawMessage(`[3.25]`),
		json.RawMessage(`["-Infinity"]`),
	}), got.Frames[0].Fields[0])
}
