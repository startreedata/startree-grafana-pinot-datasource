package pinotlib

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSqlObjectExpr(t *testing.T) {
	assert.Equal(t, SqlExpr(`"object"`), ObjectExpr("object"))
}

func TestSqlLiteralString(t *testing.T) {
	assert.Equal(t, SqlExpr(`'string'`), StringLiteralExpr("string"))
}

func TestLiteralExpr(t *testing.T) {
	t.Run("string", func(t *testing.T) { assert.Equal(t, SqlExpr(`'string'`), LiteralExpr("string")) })
	t.Run("int(1)", func(t *testing.T) { assert.Equal(t, SqlExpr(`1`), LiteralExpr(int(1))) })
	t.Run("int32(1)", func(t *testing.T) { assert.Equal(t, SqlExpr(`1`), LiteralExpr(int32(1))) })
	t.Run("int64(1)", func(t *testing.T) { assert.Equal(t, SqlExpr(`1`), LiteralExpr(int64(1))) })
	t.Run("float32(1.1)", func(t *testing.T) { assert.Equal(t, SqlExpr(`1.1`), LiteralExpr(float32(1.1))) })
	t.Run("float64(1.1)", func(t *testing.T) { assert.Equal(t, SqlExpr(`1.1`), LiteralExpr(float64(1.1))) })
	t.Run("true", func(t *testing.T) { assert.Equal(t, SqlExpr(`TRUE`), LiteralExpr(true)) })
	t.Run("false", func(t *testing.T) { assert.Equal(t, SqlExpr(`FALSE`), LiteralExpr(false)) })
}

func TestUnquoteObjectName(t *testing.T) {
	testCases := []struct {
		name string
		want string
	}{
		{name: `"object"`, want: `object`},
		{name: "`object`", want: `object`},
		{name: "object", want: `object`},
		{name: `"object`, want: `"object`},
		{name: `object"`, want: `object"`},
		{name: "`object", want: "`object"},
		{name: "object`", want: "object`"},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, UnquoteObjectName(tt.name))
		})
	}
}

func TestUnquoteStringLiteral(t *testing.T) {
	testCases := []struct {
		name string
		want string
	}{
		{name: `'object'`, want: `object`},
		{name: `object`, want: `object`},
		{name: `'object`, want: `'object`},
		{name: `object'`, want: `object'`},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, UnquoteStringLiteral(tt.name))
		})
	}
}

func TestTimeFilterExpr(t *testing.T) {
	got := TimeFilterExpr(TimeFilter{
		Column: "time",
		Format: DateTimeFormat{
			Size:   1,
			Unit:   TimeUnitSeconds,
			Format: TimeFormatEpoch,
		},
		From: time.Unix(1, 0),
		To:   time.Unix(3601, 0),
	})
	assert.Equal(t, SqlExpr(`"time" >= 1 AND "time" < 3601`), got)
}

func TestTimeFilterBucketAlignedExpr(t *testing.T) {
	timeFormat := DateTimeFormat{
		Size:   1,
		Unit:   TimeUnitSeconds,
		Format: TimeFormatEpoch,
	}
	columnName := "time"

	testArgs := []struct {
		name        string
		from        time.Time
		to          time.Time
		granularity time.Duration
		want        SqlExpr
	}{
		{
			name:        "from=0,to=3599,granularity=millisecond",
			from:        time.Unix(0, 0),
			to:          time.Unix(3599, 0),
			granularity: time.Millisecond,
			want:        `"time" >= 0 AND "time" < 3599`,
		},
		{
			name:        "from=0,to=3599,granularity=second",
			from:        time.Unix(0, 0),
			to:          time.Unix(3599, 0),
			granularity: time.Second,
			want:        `"time" >= 0 AND "time" < 3599`,
		},
		{
			name:        "from=0,to=3599,granularity=minute",
			from:        time.Unix(0, 0),
			to:          time.Unix(3599, 0),
			granularity: time.Minute,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=0,to=3599,granularity=hour",
			from:        time.Unix(0, 0),
			to:          time.Unix(3599, 0),
			granularity: time.Hour,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=0,to=3600,granularity=millisecond",
			from:        time.Unix(0, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Millisecond,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=0,to=3600,granularity=second",
			from:        time.Unix(0, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Second,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=0,to=3600,granularity=minute",
			from:        time.Unix(0, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Minute,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=0,to=3600,granularity=hour",
			from:        time.Unix(0, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Hour,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=1,to=3600,granularity=millisecond",
			from:        time.Unix(1, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Millisecond,
			want:        `"time" >= 1 AND "time" < 3600`,
		},
		{
			name:        "from=1,to=3600,granularity=second",
			from:        time.Unix(1, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Second,
			want:        `"time" >= 1 AND "time" < 3600`,
		},
		{
			name:        "from=1,to=3600,granularity=minute",
			from:        time.Unix(1, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Minute,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=1,to=3600,granularity=hour",
			from:        time.Unix(1, 0),
			to:          time.Unix(3600, 0),
			granularity: time.Hour,
			want:        `"time" >= 0 AND "time" < 3600`,
		},
		{
			name:        "from=1,to=3601,granularity=millisecond",
			from:        time.Unix(1, 0),
			to:          time.Unix(3601, 0),
			granularity: time.Millisecond,
			want:        `"time" >= 1 AND "time" < 3601`,
		},
		{
			name:        "from=1,to=3601,granularity=second",
			from:        time.Unix(1, 0),
			to:          time.Unix(3601, 0),
			granularity: time.Second,
			want:        `"time" >= 1 AND "time" < 3601`,
		},
		{
			name:        "from=1,to=3601,granularity=minute",
			from:        time.Unix(1, 0),
			to:          time.Unix(3601, 0),
			granularity: time.Minute,
			want:        `"time" >= 0 AND "time" < 3660`,
		},
		{
			name:        "from=1,to=3601,granularity=hour",
			from:        time.Unix(1, 0),
			to:          time.Unix(3601, 0),
			granularity: time.Hour,
			want:        `"time" >= 0 AND "time" < 7200`,
		},
	}

	for _, tt := range testArgs {
		t.Run(tt.name, func(t *testing.T) {
			got := TimeFilterBucketAlignedExpr(TimeFilter{
				Column: columnName,
				Format: timeFormat,
				From:   tt.from,
				To:     tt.to,
			}, tt.granularity)
			assert.Equal(t, tt.want, got)

		})
	}
}

func TestTimeExpr(t *testing.T) {
	testCases := []struct {
		format string
		ts     time.Time
		want   SqlExpr
	}{
		{ts: time.Unix(3600, 0), format: "EPOCH_NANOS", want: `3600000000000`},
		{ts: time.Unix(3600, 0), format: "1:NANOSECONDS:EPOCH", want: `3600000000000`},
		{ts: time.Unix(3600, 0), format: "2:NANOSECONDS:EPOCH", want: `1800000000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|NANOSECONDS", want: `3600000000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|NANOSECONDS|1", want: `3600000000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|NANOSECONDS|2", want: `1800000000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH_MICROS", want: `3600000000`},
		{ts: time.Unix(3600, 0), format: "1:MICROSECONDS:EPOCH", want: `3600000000`},
		{ts: time.Unix(3600, 0), format: "2:MICROSECONDS:EPOCH", want: `1800000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MICROSECONDS", want: `3600000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MICROSECONDS|1", want: `3600000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MICROSECONDS|2", want: `1800000000`},
		{ts: time.Unix(3600, 0), format: "EPOCH_MILLIS", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "1:MILLISECONDS:EPOCH", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "2:MILLISECONDS:EPOCH", want: `1800000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MILLISECONDS", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MILLISECONDS|1", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MILLISECONDS|2", want: `1800000`},
		{ts: time.Unix(3600, 0), format: "EPOCH", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "TIMESTAMP", want: `3600000`},
		{ts: time.Unix(3600, 0), format: "EPOCH_SECONDS", want: `3600`},
		{ts: time.Unix(3600, 0), format: "1:SECONDS:EPOCH", want: `3600`},
		{ts: time.Unix(3600, 0), format: "2:SECONDS:EPOCH", want: `1800`},
		{ts: time.Unix(3600, 0), format: "EPOCH|SECONDS", want: `3600`},
		{ts: time.Unix(3600, 0), format: "EPOCH|SECONDS|1", want: `3600`},
		{ts: time.Unix(3600, 0), format: "EPOCH|SECONDS|2", want: `1800`},
		{ts: time.Unix(3600, 0), format: "EPOCH_MINUTES", want: `60`},
		{ts: time.Unix(3600, 0), format: "1:MINUTES:EPOCH", want: `60`},
		{ts: time.Unix(3600, 0), format: "2:MINUTES:EPOCH", want: `30`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MINUTES", want: `60`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MINUTES|1", want: `60`},
		{ts: time.Unix(3600, 0), format: "EPOCH|MINUTES|2", want: `30`},
		{ts: time.Unix(3600, 0), format: "EPOCH_HOURS", want: `1`},
		{ts: time.Unix(3600, 0), format: "1:HOURS:EPOCH", want: `1`},
		{ts: time.Unix(3600, 0), format: "2:HOURS:EPOCH", want: `0`},
		{ts: time.Unix(3600, 0), format: "EPOCH|HOURS", want: `1`},
		{ts: time.Unix(3600, 0), format: "EPOCH|HOURS|1", want: `1`},
		{ts: time.Unix(3600, 0), format: "EPOCH|HOURS|2", want: `0`},
		{ts: time.Unix(24*5*3600, 0), format: "EPOCH_DAYS", want: `5`},
		{ts: time.Unix(24*5*3600, 0), format: "1:DAYS:EPOCH", want: `5`},
		{ts: time.Unix(24*5*3600, 0), format: "2:DAYS:EPOCH", want: `2`},
		{ts: time.Unix(24*5*3600, 0), format: "EPOCH|DAYS", want: `5`},
		{ts: time.Unix(24*5*3600, 0), format: "EPOCH|DAYS|1", want: `5`},
		{ts: time.Unix(24*5*3600, 0), format: "EPOCH|DAYS|2", want: `2`},
	}

	for _, tt := range testCases {
		t.Run("format="+tt.format, func(t *testing.T) {
			format, err := ParseDateTimeFormat(tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, TimeExpr(tt.ts, format))
		})
	}
}

func TestTimeGroupExpr(t *testing.T) {
	tableConfig := ListTableConfigsResponse{
		TableTypeRealTime: TableConfig{
			IngestionConfig: IngestionConfig{
				TransformConfigs: []TransformConfig{
					{
						ColumnName:        "ts_1m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 1), 1)`,
					}, {
						ColumnName:        "ts_2m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 2), 2)`,
					}, {
						ColumnName:        "ts_5m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 5), 5)`,
					}, {
						ColumnName:        "ts_10m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 10), 10)`,
					}, {
						ColumnName:        "ts_15m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 15), 15)`,
					}, {
						ColumnName:        "ts_30m",
						TransformFunction: `FromEpochMinutesBucket(ToEpochMinutesBucket("ts", 30), 30)`,
					}, {
						ColumnName:        "ts_1h",
						TransformFunction: `FromEpochHoursBucket(ToEpochHoursBucket("ts", 1), 1)`,
					}, {
						ColumnName:        "ts_1d",
						TransformFunction: `FromEpochDaysBucket(ToEpochDaysBucket("ts", 1), 1)`,
					},
				},
			},
		},
	}

	const outputFormat = "1:MILLISECONDS:EPOCH"
	testCases := []struct {
		format      string
		granularity string
		want        SqlExpr
	}{
		{granularity: "3:MINUTES", format: "EPOCH_NANOS", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:NANOSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:NANOSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|NANOSECONDS", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|NANOSECONDS|1", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|NANOSECONDS|2", want: `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_MICROS", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:MICROSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:MICROSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MICROSECONDS", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MICROSECONDS|1", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MICROSECONDS|2", want: `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_MILLIS", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:MILLISECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MILLISECONDS", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MILLISECONDS|2", want: `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "TIMESTAMP", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_SECONDS", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:SECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:SECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|SECONDS", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|SECONDS|1", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|SECONDS|2", want: `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_MINUTES", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:MINUTES:EPOCH", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:MINUTES:EPOCH", want: `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MINUTES", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MINUTES|1", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|MINUTES|2", want: `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_HOURS", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:HOURS:EPOCH", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:HOURS:EPOCH", want: `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|HOURS", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|HOURS|1", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|HOURS|2", want: `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH_DAYS", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "1:DAYS:EPOCH", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "2:DAYS:EPOCH", want: `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|DAYS", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|DAYS|1", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "3:MINUTES", format: "EPOCH|DAYS|2", want: `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '3:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_MILLIS", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "EPOCH_MILLIS", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "EPOCH_MILLIS", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "EPOCH_MILLIS", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "EPOCH_MILLIS", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "EPOCH_MILLIS", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "EPOCH_MILLIS", want: `"ts_1d"`},
		{granularity: "1:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "1:MILLISECONDS:EPOCH", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "1:MILLISECONDS:EPOCH", want: `"ts_1d"`},
		{granularity: "1:MINUTES", format: "TIMESTAMP", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "TIMESTAMP", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "TIMESTAMP", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "TIMESTAMP", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "TIMESTAMP", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "TIMESTAMP", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "TIMESTAMP", want: `"ts_1d"`},
		{granularity: "1:MINUTES", format: "EPOCH", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "EPOCH", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "EPOCH", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "EPOCH", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "EPOCH", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "EPOCH", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "EPOCH", want: `"ts_1d"`},
		{granularity: "1:MINUTES", format: "EPOCH|MILLISECONDS", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "EPOCH|MILLISECONDS", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "EPOCH|MILLISECONDS", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "EPOCH|MILLISECONDS", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "EPOCH|MILLISECONDS", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "EPOCH|MILLISECONDS", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "EPOCH|MILLISECONDS", want: `"ts_1d"`},
		{granularity: "1:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `"ts_1m"`},
		{granularity: "2:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `"ts_2m"`},
		{granularity: "5:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `"ts_5m"`},
		{granularity: "15:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `"ts_15m"`},
		{granularity: "30:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `"ts_30m"`},
		{granularity: "1:HOURS", format: "EPOCH|MILLISECONDS|1", want: `"ts_1h"`},
		{granularity: "1:DAYS", format: "EPOCH|MILLISECONDS|1", want: `"ts_1d"`},
		{granularity: "1:MILLISECONDS", format: "1:MILLISECONDS:EPOCH", want: `"ts"`},
		{granularity: "1:MILLISECONDS", format: "EPOCH|MILLISECONDS", want: `"ts"`},
		{granularity: "1:MILLISECONDS", format: "EPOCH|MILLISECONDS|1", want: `"ts"`},
		{granularity: "1:MILLISECONDS", format: "EPOCH", want: `"ts"`},
		{granularity: "1:MILLISECONDS", format: "TIMESTAMP", want: `"ts"`},
	}

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("format=%s,granularity=%s", tt.format, tt.granularity), func(t *testing.T) {
			group, err := DateTimeConversionOf("ts", tt.format, outputFormat, tt.granularity)
			require.NoError(t, err)
			assert.Equal(t, tt.want, TimeGroupExpr(tableConfig, group))
		})
	}
}

func TestJsonExtractExpr(t *testing.T) {
	assert.Equal(t, SqlExpr(`JSONEXTRACTSCALAR("col", '$.key1', 'STRING', '')`),
		JsonExtractScalarExpr(`"col"`, "$.key1", "STRING", `''`))
}

func TestRegexpExtractExpr(t *testing.T) {
	assert.Equal(t, SqlExpr(`REGEXPEXTRACT("col", '(\w+) (\w+)', 0, '')`),
		RegexpExtractExpr(`"col"`, `(\w+) (\w+)`, 0, `''`))
}

func TestQueryOptionExpr(t *testing.T) {
	assert.Equal(t, SqlExpr(`SET myOption=true;`), QueryOptionExpr("myOption", "true"))
}
