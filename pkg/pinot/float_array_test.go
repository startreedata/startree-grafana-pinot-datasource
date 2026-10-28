package pinot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractFloatArray(t *testing.T) {
	var results ResultTable
	decoder := json.NewDecoder(strings.NewReader(`{
		"dataSchema": {"columnNames": ["readings"], "columnDataTypes": ["FLOAT_ARRAY"]},
		"rows": [[[1.5, -2.25, 0]], [[]], [null], [["NaN", "Infinity", "-Infinity", 3.4028235E38]]]
	}`))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&results))

	column, err := ExtractColumn(&results, 0)
	require.NoError(t, err)
	values, ok := column.([]json.RawMessage)
	require.True(t, ok)
	require.Len(t, values, 4)
	want := []string{`[1.5,-2.25,0]`, `[]`, `null`, `["NaN","Infinity","-Infinity",3.4028235E38]`}
	for idx := range want {
		assert.JSONEq(t, want[idx], string(values[idx]))
	}

	labels, err := ExtractColumnAsStrings(&results, 0)
	require.NoError(t, err)
	assert.Equal(t, want, labels)

	expressions, err := ExtractColumnAsExprs(&results, 0)
	require.ErrorContains(t, err, "FLOAT_ARRAY SQL expressions are not supported")
	assert.Nil(t, expressions)

	doubles, err := ExtractColumnAsDoubles(&results, 0)
	require.Error(t, err)
	assert.Nil(t, doubles)
}

func TestExtractFloatArrayEmptyResult(t *testing.T) {
	results := &ResultTable{DataSchema: DataSchema{ColumnDataTypes: []string{DataTypeFloatArray}}}
	column, err := ExtractColumn(results, 0)
	require.NoError(t, err)
	assert.Equal(t, []json.RawMessage{}, column)
}

func TestExtractFloatArrayRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		err   string
	}{
		{"scalar", json.Number("1.5"), "expected float array"},
		{"string", []any{json.Number("1"), "abc"}, "expected float at array index 1"},
		{"bool", []any{true}, "expected float at array index 0"},
		{"null element", []any{nil}, "expected float at array index 0"},
		{"nested array", []any{[]any{json.Number("1")}}, "expected float at array index 0"},
		{"object", map[string]any{"value": json.Number("1")}, "expected float array"},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := &ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{DataTypeString, DataTypeFloatArray}},
				Rows:       [][]any{{"ok", []any{json.Number("1.5")}}, {"invalid", test.value}},
			}
			column, err := ExtractColumn(results, 1)
			require.ErrorContains(t, err, test.err)
			var extractionError *ExtractorError
			require.ErrorAs(t, err, &extractionError)
			assert.Equal(t, 1, extractionError.RowIdx)
			assert.Equal(t, 1, extractionError.ColumnIdx)
			assert.Nil(t, column)
		})
	}
}
