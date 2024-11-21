package pinotlib

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		SchemaName: "benchmark",
		DimensionFieldSpecs: []DimensionFieldSpec{
			{Name: "fabric", DataType: "STRING"},
			{Name: "pattern", DataType: "STRING"},
		},
		MetricFieldSpecs: []MetricFieldSpec{{
			Name:     "value",
			DataType: "DOUBLE",
		}},
		DateTimeFieldSpecs: []DateTimeFieldSpec{{
			Name:        "ts",
			DataType:    "TIMESTAMP",
			Format:      "TIMESTAMP",
			Granularity: "1:MILLISECONDS",
		}},
	}

	testArgs := []struct {
		colName string
		want    DateTimeFormat
		wantErr bool
	}{
		{colName: "ts", want: DateTimeFormatMillisecondsEpoch()},
		{colName: "fabric", wantErr: true},
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
	client := setupPinotAndCreateClient(t)

	testCases := []struct {
		column string
		want   interface{}
	}{
		{column: "__double", want: interface{}([]float64{0, 0.1111111111111111, 0.2222222222222222})},
		{column: "__float", want: interface{}([]float64{0, 0.11111111, 0.22222222})},
		{column: "__int", want: interface{}([]int64{0, 111_111, 222_222})},
		{column: "__long", want: interface{}([]int64{0, 111111111111111, 222222222222222})},
		{column: "__string", want: interface{}([]string{"row_0", "row_1", "row_2"})},
		{column: "__bytes", want: interface{}([]string{"8445a8345a43b74d9d130cbf28dbeff9", "6373c2b93fb7c22bd893c3bdfeac70f4", "094174861955e2918de963f0103a065a"})},
		{column: "__json", want: interface{}([]string{"{\"key1\":\"val1\",\"key2\":2,\"key3\":[\"val3_1\",\"val3_2\"]}", "{\"key1\":\"val1\",\"key2\":2,\"key3\":[\"val3_1\",\"val3_2\"]}", "{\"key1\":\"val1\",\"key2\":2,\"key3\":[\"val3_1\",\"val3_2\"]}"})},
		{column: "__bool", want: interface{}([]bool{true, false, true})},
		{column: "__timestamp", want: interface{}([]time.Time{time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, time.November, 1, 0, 0, 1, 0, time.UTC), time.Date(2024, time.November, 1, 0, 0, 2, 0, time.UTC)})},
		{column: "__map_string_long", want: interface{}([]map[string]interface{}{{"key1": json.Number("1"), "key2": json.Number("2")}, {"key1": json.Number("1"), "key2": json.Number("2")}, {"key1": json.Number("1"), "key2": json.Number("2")}})},
		{column: "__map_string_string", want: interface{}([]map[string]interface{}{{"key1": "val1", "key2": "val2"}, {"key1": "val1", "key2": "val2"}, {"key1": "val1", "key2": "val2"}})},
	}

	resp, err := client.ExecuteSqlQuery(context.Background(),
		NewSqlQuery(`select * from "allDataTypes" order by "__timestamp" asc limit 3`))
	require.NoError(t, err, "client.ExecuteSqlQuery()")
	require.True(t, resp.HasData(), "resp.HasData()")

	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			assert.Equal(t, tt.want, ExtractColumn(resp.ResultTable, colIdx))
		})
	}
}

func TestParseJodaTime(t *testing.T) {
	testCases := []struct {
		ts      string
		want    time.Time
		wantErr bool
	}{
		{ts: "", wantErr: true},
		{ts: "2024-10-24 10:11:12.1", want: time.Date(2024, 10, 24, 10, 11, 12, 0.1e9, time.UTC)},
		{ts: "2024-10-25 10:11:12.01", want: time.Date(2024, 10, 25, 10, 11, 12, 0.01e9, time.UTC)},
		{ts: "2024-10-26 15:11:12.001", want: time.Date(2024, 10, 26, 15, 11, 12, 0.001e9, time.UTC)},
	}

	for _, tt := range testCases {
		t.Run(tt.ts, func(t *testing.T) {
			got, err := ParseJodaTime(tt.ts)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tt.want, got)
				assert.NoError(t, err)
			}
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
		{
			dataType: DataTypeTimestamp,
			col:      []interface{}{"2024-10-24 10:11:12.1", "2024-10-25 10:11:12.01", "2024-10-26 10:11:12.001"},
			want:     []int64{1729764672100, 1729851072010, 1729937472001},
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
		{
			dataType: DataTypeTimestamp,
			col:      []interface{}{"2024-10-24 10:11:12.1", "2024-10-25 10:11:12.01", "2024-10-26 10:11:12.001"},
			want:     []float64{1729764672100, 1729851072010, 1729937472001},
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
		{
			dataType: DataTypeTimestamp,
			col:      []interface{}{"2024-10-24 10:11:12.1", "2024-10-25 10:11:12.01", "2024-10-26 10:11:12.001"},
			want:     []string{"2024-10-24 10:11:12.1", "2024-10-25 10:11:12.01", "2024-10-26 10:11:12.001"},
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

func TestExtractTimeColumn(t *testing.T) {
	tests := []struct {
		format   string
		col      []interface{}
		dataType string
		want     []time.Time
	}{
		{
			format:   "EPOCH_NANOS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format:   "1:NANOSECONDS:EPOCH",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format:   "EPOCH|NANOSECONDS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format:   "EPOCH|NANOSECONDS|1",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541).UTC()},
		}, {
			format:   "EPOCH_MICROS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format:   "1:MICROSECONDS:EPOCH",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format:   "EPOCH|MICROSECONDS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format:   "EPOCH|MICROSECONDS|1",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds()).UTC()},
		}, {
			format:   "EPOCH_SECONDS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541, 0).UTC()},
		}, {
			format:   "1:SECONDS:EPOCH",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541, 0).UTC()},
		}, {
			format:   "EPOCH|SECONDS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541, 0).UTC()},
		}, {
			format:   "EPOCH|SECONDS|1",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541, 0).UTC()},
		}, {
			format:   "EPOCH_MINUTES",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*60, 0).UTC()},
		}, {
			format:   "1:MINUTES:EPOCH",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*60, 0).UTC()},
		}, {
			format:   "EPOCH|MINUTES",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*60, 0).UTC()},
		}, {
			format:   "EPOCH|MINUTES|1",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*60, 0).UTC()},
		}, {
			format:   "EPOCH_HOURS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		}, {
			format:   "1:HOURS:EPOCH",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		}, {
			format:   "EPOCH|HOURS",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		}, {
			format:   "EPOCH|HOURS|1",
			col:      []interface{}{json.Number("11721075541")},
			dataType: DataTypeLong,
			want:     []time.Time{time.Unix(11721075541*3600, 0).UTC()},
		}, {
			format:   "TIMESTAMP",
			col:      []interface{}{"2024-10-24 10:11:12.1", "2024-10-25 10:11:12.01", "2024-10-26 15:11:12.001"},
			dataType: DataTypeTimestamp,
			want: []time.Time{
				time.Date(2024, 10, 24, 10, 11, 12, 0.1e9, time.UTC),
				time.Date(2024, 10, 25, 10, 11, 12, 0.01e9, time.UTC),
				time.Date(2024, 10, 26, 15, 11, 12, 0.001e9, time.UTC),
			},
		},
	}

	for _, tt := range tests {
		t.Run("format="+tt.format, func(t *testing.T) {
			format, err := ParseDateTimeFormat(tt.format)
			require.NoError(t, err)

			got, err := ExtractTimeColumn(&ResultTable{
				DataSchema: DataSchema{ColumnDataTypes: []string{tt.dataType}},
				Rows:       reshapeCols(tt.col),
			}, 0, format)
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
