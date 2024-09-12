package dataquery

import (
	"encoding/json"
	"github.com/startreedata/pinot-client-go/pinot"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetColumnIdx(t *testing.T) {
	want := 1
	got, err := GetColumnIdx(&pinot.ResultTable{
		DataSchema: pinot.RespSchema{ColumnNames: []string{"col0", "col1"}},
	}, "col1")
	assert.NoError(t, err)
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
			want:   []time.Time{time.Unix(0, 11721075541)},
		}, {
			format: "1:NANOSECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541)},
		}, {
			format: "EPOCH|NANOSECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541)},
		}, {
			format: "EPOCH|NANOSECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541)},
		},
		{
			format: "EPOCH_MICROS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds())},
		}, {
			format: "1:MICROSECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds())},
		}, {
			format: "EPOCH|MICROSECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds())},
		}, {
			format: "EPOCH|MICROSECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(0, 11721075541*time.Microsecond.Nanoseconds())},
		},
		{
			format: "EPOCH_SECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0)},
		},
		{
			format: "1:SECONDS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0)},
		},
		{
			format: "EPOCH|SECONDS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0)},
		},
		{
			format: "EPOCH|SECONDS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541, 0)},
		},
		{
			format: "EPOCH_MINUTES",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0)},
		},
		{
			format: "1:MINUTES:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0)},
		},
		{
			format: "EPOCH|MINUTES",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0)},
		},
		{
			format: "EPOCH|MINUTES|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*60, 0)},
		},
		{
			format: "EPOCH_HOURS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0)},
		},
		{
			format: "1:HOURS:EPOCH",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0)},
		},
		{
			format: "EPOCH|HOURS",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0)},
		},
		{
			format: "EPOCH|HOURS|1",
			rows:   [][]interface{}{{json.Number("11721075541")}},
			want:   []time.Time{time.Unix(11721075541*3600, 0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			got, err := ExtractLongTimeColumn(&pinot.ResultTable{
				DataSchema: pinot.RespSchema{ColumnDataTypes: []string{"LONG"}},
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
