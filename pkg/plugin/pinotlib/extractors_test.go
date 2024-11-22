package pinotlib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/big"
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
	exp20 := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(20), nil)

	testCases := []struct {
		column string
		want   interface{}
	}{
		{column: "__double", want: interface{}([]float64{0, 0.1111111111111111, 0.2222222222222222})},
		{column: "__float", want: interface{}([]float32{0, 0.11111111, 0.22222222})},
		{column: "__int", want: interface{}([]int32{0, 111111, 222222})},
		{column: "__long", want: interface{}([]int64{0, 111111111111111, 222222222222222})},
		{column: "__string", want: interface{}([]string{"row_0", "row_1", "row_2"})},
		{column: "__bytes", want: interface{}([][]byte{[]byte("row_0"), []byte("row_1"), []byte("row_2")})},
		{column: "__bool", want: interface{}([]bool{true, false, true})},
		{column: "__big_decimal", want: interface{}([]*big.Int{
			big.NewInt(0).Add(exp20, big.NewInt(0)),
			big.NewInt(0).Add(exp20, big.NewInt(1)),
			big.NewInt(0).Add(exp20, big.NewInt(2)),
		})},
		{column: "__json", want: interface{}([]json.RawMessage{
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`),
			json.RawMessage(`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`)})},
		{column: "__timestamp", want: interface{}([]time.Time{
			time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 1, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 2, 0, time.UTC)})},
		{column: "__map_string_long", want: interface{}([]map[string]interface{}{
			{"key1": json.Number("1"), "key2": json.Number("2")},
			{"key1": json.Number("1"), "key2": json.Number("2")},
			{"key1": json.Number("1"), "key2": json.Number("2")}})},
		{column: "__map_string_string", want: interface{}([]map[string]interface{}{
			{"key1": "val1", "key2": "val2"},
			{"key1": "val1", "key2": "val2"},
			{"key1": "val1", "key2": "val2"}})},
	}

	resp := selectStarFromAllDataTypes(t, 3)
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

func TestExtractColumnAsDoubles(t *testing.T) {
	testCases := []struct {
		column  string
		want    []float64
		wantErr error
	}{
		{column: "__double", want: []float64{0, 0.1111111111111111, 0.2222222222222222}},
		{column: "__float", want: []float64{0, 0.11111111, 0.22222222}},
		{column: "__int", want: []float64{0, 111111, 222222}},
		{column: "__long", want: []float64{0, 111111111111111, 222222222222222}},
		{column: "__big_decimal", want: []float64{1e20, 1e20 + 1, 1e20 + 2}},
		{column: "__bool", wantErr: errors.New("not a numeric column")},
		{column: "__string", wantErr: errors.New("not a numeric column")},
		{column: "__bytes", wantErr: errors.New("not a numeric column")},
		{column: "__json", wantErr: errors.New("not a numeric column")},
		{column: "__timestamp", wantErr: errors.New("not a numeric column")},
		{column: "__map_string_long", wantErr: errors.New("not a numeric column")},
		{column: "__map_string_string", wantErr: errors.New("not a numeric column")},
	}

	resp := selectStarFromAllDataTypes(t, 3)
	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			got, gotErr := ExtractColumnAsDoubles(resp.ResultTable, colIdx)
			assert.Equal(t, tt.want, got)
			if tt.wantErr != nil {
				assert.EqualError(t, gotErr, tt.wantErr.Error())
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func TestExtractColumnAsStrings(t *testing.T) {
	testCases := []struct {
		column string
		want   []string
	}{
		{column: "__double", want: []string{"0", "0.1111111111111111", "0.2222222222222222"}},
		{column: "__float", want: []string{"0", "0.11111111", "0.22222222"}},
		{column: "__int", want: []string{"0", "111111", "222222"}},
		{column: "__long", want: []string{"0", "111111111111111", "222222222222222"}},
		{column: "__bool", want: []string{"true", "false", "true"}},
		{column: "__string", want: []string{"row_0", "row_1", "row_2"}},
		{column: "__bytes", want: []string{"row_0", "row_1", "row_2"}},
		{column: "__big_decimal", want: []string{"100000000000000000000", "100000000000000000001", "100000000000000000002"}},
		{column: "__json", want: []string{
			`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`,
			`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`,
			`{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}`}},
		{column: "__timestamp", want: []string{
			"2024-11-01 00:00:00.0", "2024-11-01 00:00:01.0", "2024-11-01 00:00:02.0"}},
		{column: "__map_string_long", want: []string{
			`{"key1":1,"key2":2}`, `{"key1":1,"key2":2}`, `{"key1":1,"key2":2}`}},
		{column: "__map_string_string", want: []string{
			`{"key1":"val1","key2":"val2"}`, `{"key1":"val1","key2":"val2"}`, `{"key1":"val1","key2":"val2"}`}},
	}

	resp := selectStarFromAllDataTypes(t, 3)
	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			assert.Equal(t, tt.want, ExtractColumnAsStrings(resp.ResultTable, colIdx))
		})
	}
}

func TestExtractColumnAsExprs(t *testing.T) {
	testCases := []struct {
		column string
		want   []string
	}{
		{column: "__double", want: []string{"0.0", "0.1111111111111111", "0.2222222222222222"}},
		{column: "__float", want: []string{"0.0", "0.11111111", "0.22222222"}},
		{column: "__int", want: []string{"0", "111111", "222222"}},
		{column: "__long", want: []string{"0", "111111111111111", "222222222222222"}},
		{column: "__bool", want: []string{"true", "false", "true"}},
		{column: "__string", want: []string{"'row_0'", "'row_1'", "'row_2'"}},
		{column: "__bytes", want: []string{"'726f775f30'", "'726f775f31'", "'726f775f32'"}},
		{column: "__big_decimal", want: []string{"'100000000000000000000'", "'100000000000000000001'", "'100000000000000000002'"}},
		{column: "__json", want: []string{
			`'{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}'`,
			`'{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}'`,
			`'{"key1":"val1","key2":2,"key3":["val3_1","val3_2"]}'`}},
		{column: "__timestamp", want: []string{"1730419200000", "1730419201000", "1730419202000"}},
	}

	resp := selectStarFromAllDataTypes(t, 3)
	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			assert.Equal(t, tt.want, ExtractColumnAsExprs(resp.ResultTable, colIdx))
		})
	}
}

func TestExtractColumnAsTime(t *testing.T) {
	testCases := []struct {
		column  string
		want    []time.Time
		wantErr error
	}{
		{column: "__long", want: []time.Time{
			time.Unix(0, 0).UTC(),
			time.Unix(111111111111, int64(111*time.Millisecond)).UTC(),
			time.Unix(222222222222, int64(222*time.Millisecond)).UTC()}},
		{column: "__timestamp", want: []time.Time{
			time.Date(2024, time.November, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 1, 0, time.UTC),
			time.Date(2024, time.November, 1, 0, 0, 2, 0, time.UTC)}},
		{column: "__int", wantErr: errors.New("not a timestamp column")},
		{column: "__double", wantErr: errors.New("not a timestamp column")},
		{column: "__float", wantErr: errors.New("not a timestamp column")},
		{column: "__string", wantErr: errors.New("not a timestamp column")},
		{column: "__bool", wantErr: errors.New("not a timestamp column")},
		{column: "__big_decimal", wantErr: errors.New("not a timestamp column")},
		{column: "__bytes", wantErr: errors.New("not a timestamp column")},
		{column: "__json", wantErr: errors.New("not a timestamp column")},
		{column: "__map_string_long", wantErr: errors.New("not a timestamp column")},
		{column: "__map_string_string", wantErr: errors.New("not a timestamp column")},
	}

	resp := selectStarFromAllDataTypes(t, 3)
	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			got, gotErr := ExtractColumnAsTime(resp.ResultTable, colIdx, DateTimeFormatMillisecondsEpoch())

			assert.Equal(t, tt.want, got)
			if tt.wantErr != nil {
				assert.EqualError(t, gotErr, tt.wantErr.Error())
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func TestDecodeJsonFromColumn(t *testing.T) {
	testCases := []struct {
		column  string
		want    []map[string]interface{}
		wantErr error
	}{
		{column: "__json", want: []map[string]interface{}{
			{"key1": "val1", "key2": json.Number("2"), "key3": []interface{}{"val3_1", "val3_2"}},
			{"key1": "val1", "key2": json.Number("2"), "key3": []interface{}{"val3_1", "val3_2"}},
			{"key1": "val1", "key2": json.Number("2"), "key3": []interface{}{"val3_1", "val3_2"}},
		}},
		{column: "__string",
			wantErr: errors.New("failed to unmarshal json at row 0, column 10: invalid character 'r' looking for beginning of value")},
		{column: "__bytes",
			wantErr: errors.New("failed to unmarshal json at row 0, column 2: invalid character 'r' looking for beginning of value")},
		{column: "__int", wantErr: errors.New("column does not contain json")},
		{column: "__long", wantErr: errors.New("column does not contain json")},
		{column: "__double", wantErr: errors.New("column does not contain json")},
		{column: "__float", wantErr: errors.New("column does not contain json")},
		{column: "__bool", wantErr: errors.New("column does not contain json")},
		{column: "__timestamp", wantErr: errors.New("column does not contain json")},
		{column: "__big_decimal", wantErr: errors.New("column does not contain json")},
		{column: "__map_string_long", wantErr: errors.New("column does not contain json")},
		{column: "__map_string_string", wantErr: errors.New("column does not contain json")},
	}

	resp := selectStarFromAllDataTypes(t, 3)
	for _, tt := range testCases {
		t.Run(tt.column, func(t *testing.T) {
			colIdx, err := GetColumnIdx(resp.ResultTable, tt.column)
			require.NoError(t, err)
			got, gotErr := DecodeJsonFromColumn[map[string]interface{}](resp.ResultTable, colIdx)

			assert.Equal(t, tt.want, got)
			if tt.wantErr != nil {
				assert.EqualError(t, gotErr, tt.wantErr.Error())
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func selectStarFromAllDataTypes(t *testing.T, limit int) *BrokerResponse {
	t.Helper()

	client := setupPinotAndCreateClient(t)
	resp, err := client.ExecuteSqlQuery(context.Background(),
		NewSqlQuery(fmt.Sprintf(`select * from "allDataTypes" order by "__timestamp" asc limit %d`, limit)))
	require.NoError(t, err, "client.ExecuteSqlQuery()")
	require.True(t, resp.HasData(), "resp.HasData()")
	return resp
}

func TestGetDistinctValues(t *testing.T) {
	got := GetDistinctValues([]int64{1, 2, 2, 2, 3, 3, 3, 4, 5, 5, 4, 3})
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, got)
}
