package pinot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractStringArray(t *testing.T) {
	var results ResultTable
	decoder := json.NewDecoder(strings.NewReader(`{
		"dataSchema": {"columnNames": ["tags"], "columnDataTypes": ["STRING_ARRAY"]},
		"rows": [[["first", "second"]], [[]], [null], [["quote\"", "line\n", "\\", "雪"]]]
	}`))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&results))

	column, err := ExtractColumn(&results, 0)
	require.NoError(t, err)
	values, ok := column.([]json.RawMessage)
	require.True(t, ok)
	require.Len(t, values, 4)
	want := []string{`["first","second"]`, `[]`, `null`, `["quote\"","line\n","\\","雪"]`}
	for idx := range want {
		assert.JSONEq(t, want[idx], string(values[idx]))
	}

	labels, err := ExtractColumnAsStrings(&results, 0)
	require.NoError(t, err)
	assert.Equal(t, want, labels)

	expressions, err := ExtractColumnAsExprs(&results, 0)
	require.ErrorContains(t, err, "STRING_ARRAY SQL expressions are not supported")
	assert.Nil(t, expressions)
}

func TestExtractStringArrayEmptyResult(t *testing.T) {
	results := &ResultTable{DataSchema: DataSchema{ColumnDataTypes: []string{DataTypeStringArray}}}
	column, err := ExtractColumn(results, 0)
	require.NoError(t, err)
	assert.Equal(t, []json.RawMessage{}, column)
}

func TestExtractStringArrayRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		err   string
	}{
		{"scalar", "not an array", "expected string array"},
		{"number", []any{"valid", json.Number("2")}, "expected string at array index 1"},
		{"null element", []any{nil}, "expected string at array index 0"},
		{"nested array", []any{[]any{"nested"}}, "expected string at array index 0"},
		{"object", map[string]any{"tag": "value"}, "expected string array"},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := &ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{DataTypeString, DataTypeStringArray}},
				Rows:       [][]any{{"ok", []any{"valid"}}, {"invalid", test.value}},
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
