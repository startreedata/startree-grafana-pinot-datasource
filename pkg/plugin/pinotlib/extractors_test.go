package pinotlib

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetColumnIdx(t *testing.T) {
	results := &ResultTable{
		DataSchema: DataSchema{ColumnNames: []string{"col0", "col1"}},
	}

	testArgs := []struct {
		colName string
		want    int
		wantErr bool
	}{
		{colName: "col0", want: 0},
		{colName: "col1", want: 1},
		{colName: "dne", want: -1, wantErr: true},
	}

	for _, tt := range testArgs {
		t.Run(tt.colName, func(t *testing.T) {
			got, err := GetColumnIdx(results, tt.colName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetTimeColumnFormat(t *testing.T) {
	schema := TableSchema{
		SchemaName: "githubEvents",
		DimensionFieldSpecs: []DimensionFieldSpec{
			{Name: "id", DataType: "STRING"},
			{Name: "type", DataType: "STRING"},
			{Name: "actor", DataType: "JSON"},
			{Name: "repo", DataType: "JSON"},
			{Name: "payload", DataType: "JSON"},
			{Name: "public", DataType: "BOOLEAN"},
		},
		MetricFieldSpecs: nil,
		DateTimeFieldSpecs: []DateTimeFieldSpec{
			{
				Name:        "created_at",
				DataType:    "STRING",
				Format:      "1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss'Z'",
				Granularity: "1:SECONDS",
			},
			{
				Name:        "created_at_timestamp",
				DataType:    "TIMESTAMP",
				Format:      "TIMESTAMP",
				Granularity: "1:SECONDS",
			},
		},
	}

	testArgs := []struct {
		colName string
		want    string
		wantErr bool
	}{
		{colName: "created_at", want: "1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss'Z'"},
		{colName: "created_at_timestamp", want: "TIMESTAMP"},
		{colName: "actor", wantErr: true},
	}

	for _, tt := range testArgs {
		t.Run(tt.colName, func(t *testing.T) {
			got, err := GetTimeColumnFormat(schema, tt.colName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractColumn(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     interface{}
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []float64{1.1, 2.2, 3.3},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []float64{1.1, 2.2, 3.3},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"a", "b", "c"},
			want:     []string{"a", "b", "c"},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
			want:     []string{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []bool{true, false, true},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractColumnExpr(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     []string
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []string{"1", "2", "3"},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []string{"1", "2", "3"},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []string{"1.1", "2.2", "3.3"},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []string{"1.1", "2.2", "3.3"},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"a", "b", "c"},
			want:     []string{"'a'", "'b'", "'c'"},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
			want:     []string{`'{"x":"a"}'`, `'{"x":"b"}'`, `'{"x":"c"}'`},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []string{"true", "false", "true"},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractColumnExpr(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractLongColumn(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     []int64
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []int64{1, 2, 3},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"a", "b", "c"},
			want:     []int64{0, 0, 0},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
			want:     []int64{0, 0, 0},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []int64{1, 0, 1},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractLongColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractDoubleColumn(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     []float64
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []float64{1, 2, 3},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []float64{1, 2, 3},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []float64{1.1, 2.2, 3.3},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []float64{1.1, 2.2, 3.3},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"a", "b", "c"},
			want:     []float64{0, 0, 0},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
			want:     []float64{0, 0, 0},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []float64{1, 0, 1},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractDoubleColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractBooleanColumn(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     []bool
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("0"), json.Number("2"), json.Number("3")},
			want:     []bool{false, true, true},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("0"), json.Number("2"), json.Number("3")},
			want:     []bool{false, true, true},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("0.0"), json.Number("2.2"), json.Number("3.3")},
			want:     []bool{false, true, true},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("0.0"), json.Number("2.2"), json.Number("3.3")},
			want:     []bool{false, true, true},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"true", "false", "c"},
			want:     []bool{true, false, false},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`true`, `false`, `{"x":"c"}`},
			want:     []bool{true, false, false},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []bool{true, false, true},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractBooleanColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractStringColumn(t *testing.T) {
	testArgs := []struct {
		dataType string
		col      []interface{}
		want     []string
	}{
		{
			dataType: DataTypeInt,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []string{"1", "2", "3"},
		},
		{
			dataType: DataTypeLong,
			col:      []interface{}{json.Number("1"), json.Number("2"), json.Number("3")},
			want:     []string{"1", "2", "3"},
		},
		{
			dataType: DataTypeFloat,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []string{"1.1", "2.2", "3.3"},
		},
		{
			dataType: DataTypeDouble,
			col:      []interface{}{json.Number("1.1"), json.Number("2.2"), json.Number("3.3")},
			want:     []string{"1.1", "2.2", "3.3"},
		},
		{
			dataType: DataTypeString,
			col:      []interface{}{"a", "b", "c"},
			want:     []string{"a", "b", "c"},
		},
		{
			dataType: DataTypeJson,
			col:      []interface{}{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
			want:     []string{`{"x":"a"}`, `{"x":"b"}`, `{"x":"c"}`},
		},
		{
			dataType: DataTypeBoolean,
			col:      []interface{}{true, false, true},
			want:     []string{"true", "false", "true"},
		},
	}

	for _, tt := range testArgs {
		t.Run("dataType="+tt.dataType, func(t *testing.T) {
			got := ExtractStringColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0)
			assert.Equal(t, tt.want, got)
		})
	}
}

func reshapeCols(cols ...[]interface{}) [][]interface{} {
	rows := make([][]interface{}, len(cols[0]))
	for i := 0; i < len(cols[0]); i++ {
		rows[i] = make([]interface{}, len(cols))
	}

	for colIdx, colValues := range cols {
		for valIdx, val := range colValues {
			rows[valIdx][colIdx] = val
		}
	}
	return rows
}

func TestGetDistinctValues(t *testing.T) {
	got := GetDistinctValues([]int64{1, 2, 2, 2, 3, 3, 3, 4, 5, 5, 4, 3})
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, got)
}

func TestExtractLongTimeColumn(t *testing.T) {
	tests := []struct {
		format string
		rows   [][]interface{}
		want   []time.Time
	}{
		{
			format: "EPOCH_NANOS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format: "1:NANOSECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format: "EPOCH|NANOSECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format: "EPOCH|NANOSECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541).UTC()},
		},
		{
			format: "EPOCH_MICROS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format: "1:MICROSECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format: "EPOCH|MICROSECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format: "EPOCH|MICROSECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		},
		{
			format: "EPOCH_SECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0).UTC()},
		},
		{
			format: "1:SECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0).UTC()},
		},
		{
			format: "EPOCH|SECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0).UTC()},
		},
		{
			format: "EPOCH|SECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0).UTC()},
		},
		{
			format: "EPOCH_MINUTES",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0).UTC()},
		},
		{
			format: "1:MINUTES:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0).UTC()},
		},
		{
			format: "EPOCH|MINUTES",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0).UTC()},
		},
		{
			format: "EPOCH|MINUTES|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0).UTC()},
		},
		{
			format: "EPOCH_HOURS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		},
		{
			format: "1:HOURS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		},
		{
			format: "EPOCH|HOURS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		},
		{
			format: "EPOCH|HOURS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			got, err := ExtractLongTimeColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{DataTypeLong}},
				Rows:       tt.rows,
			}, 0, tt.format)
			assert.NoError(t, err)
			assertEqualTimeColumns(t, tt.want, got)
		})
	}
}

func assertEqualTimeColumns(t *testing.T, want, got []time.Time) {
	wantStrs := make([]string, len(want))
	gotStrs := make([]string, len(got))

	for i := range want {
		wantStrs[i] = want[i].String()
	}
	for i := range got {
		gotStrs[i] = got[i].String()
	}
	assert.Equal(t, wantStrs, gotStrs)
}
