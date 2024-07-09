package plugin

import (
	"encoding/json"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetColumnIdx(t *testing.T) {
	want := 1
	got, ok := GetColumnIdx(&pinot.ResultTable{
		DataSchema: pinot.RespSchema{ColumnNames: []string{"col0", "col1"}},
	}, "col1")
	assert.True(t, ok)
	assert.Equal(t, want, got)
}

func TestExtractDoubleColumn(t *testing.T) {
	got := ExtractDoubleColumn(&pinot.ResultTable{
		DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"DOUBLE"}},
		Rows:       [][]interface{}{{json.Number("1.1")}, {json.Number("2.2")}, {json.Number("3.3")}},
	}, 0)
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, got)
}

func TestExtractLongColumn(t *testing.T) {
	got := ExtractLongColumn(&pinot.ResultTable{
		DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"LONG"}},
		Rows:       [][]interface{}{{json.Number("1")}, {json.Number("2")}, {json.Number("3")}},
	}, 0)
	assert.Equal(t, []int64{1, 2, 3}, got)
}

func TestExtractStringColumn(t *testing.T) {
	t.Run("type=STRING", func(t *testing.T) {
		got := ExtractStringColumn(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"STRING"}},
			Rows:       [][]interface{}{{"a"}, {"b"}, {"c"}},
		}, 0)
		assert.Equal(t, []string{"a", "b", "c"}, got)
	})
	t.Run("type=LONG", func(t *testing.T) {
		got := ExtractStringColumn(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"LONG"}},
			Rows:       [][]interface{}{{json.Number("1")}, {json.Number("2")}, {json.Number("3")}},
		}, 0)
		assert.Equal(t, []string{"1", "2", "3"}, got)
	})
	t.Run("type=DOUBLE", func(t *testing.T) {
		got := ExtractStringColumn(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"DOUBLE"}},
			Rows:       [][]interface{}{{json.Number("1.1")}, {json.Number("2.2")}, {json.Number("3.3")}},
		}, 0)
		assert.Equal(t, []string{"1.1", "2.2", "3.3"}, got)
	})
}

func TestExtractColumnExpr(t *testing.T) {
	t.Run("type=STRING", func(t *testing.T) {
		got := ExtractColumnExpr(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"STRING"}},
			Rows:       [][]interface{}{{"a"}, {"b"}, {"c"}},
		}, 0)
		assert.Equal(t, []string{"'a'", "'b'", "'c'"}, got)
	})
	t.Run("type=LONG", func(t *testing.T) {
		got := ExtractColumnExpr(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"LONG"}},
			Rows:       [][]interface{}{{json.Number("1")}, {json.Number("2")}, {json.Number("3")}},
		}, 0)
		assert.Equal(t, []string{"1", "2", "3"}, got)
	})
	t.Run("type=DOUBLE", func(t *testing.T) {
		got := ExtractColumnExpr(&pinot.ResultTable{
			DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"DOUBLE"}},
			Rows:       [][]interface{}{{json.Number("1.1")}, {json.Number("2.2")}, {json.Number("3.3")}},
		}, 0)
		assert.Equal(t, []string{"1.1", "2.2", "3.3"}, got)
	})
}
