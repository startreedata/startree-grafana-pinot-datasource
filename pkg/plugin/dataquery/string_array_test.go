package dataquery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/require"
)

func TestStringArrayTableFrameRoundTrip(t *testing.T) {
	// Decode a broker response to exercise the same value types as live queries.
	const response = `{"dataSchema":{"columnNames":["eventName","attributeNames"],"columnDataTypes":["STRING","STRING_ARRAY"]},"rows":[["checkout",["quote\"","line\n","雪"]],["empty",[]],["missing",null]]}`
	var result pinot.ResultTable
	decoder := json.NewDecoder(strings.NewReader(response))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&result))
	frame, err := ExtractTableDataFrame(&result, "")
	require.NoError(t, err)
	require.Len(t, frame.Fields, 2)
	require.Equal(t, "checkout", frame.Fields[0].At(0))
	want := []string{`["quote\"","line\n","雪"]`, `[]`, `null`}
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
	require.Contains(t, string(jsonFrame), `"attributeNames"`)
}
