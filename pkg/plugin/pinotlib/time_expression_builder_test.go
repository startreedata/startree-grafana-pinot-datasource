package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSqlObjectExpr(t *testing.T) {
	assert.Equal(t, `"object"`, SqlObjectExpr("object"))
}

func TestSqlLiteralString(t *testing.T) {
	assert.Equal(t, `'string'`, SqlLiteralStringExpr("string"))
}

func TestNewTimeExpressionBuilder(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	assert.NoError(t, err)
	assert.NotNil(t, exprBuilder)
	assert.Equal(t, "1:SECONDS:EPOCH", exprBuilder.timeColumnFormat)
	assert.Equal(t, "time", exprBuilder.timeColumn)
}

func TestTimeExpressionBuilder_TimeFilterExpr(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	require.NoError(t, err)

	got := exprBuilder.TimeFilterExpr(time.Unix(1, 0), time.Unix(3601, 0))
	assert.Equal(t, `"time" >= 1 AND "time" < 3601`, got)
}

func TestTimeExpressionBuilder_TimeFilterBucketAlignedExpr(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	require.NoError(t, err)

	testArgs := []struct {
		name        string
		from        time.Time
		to          time.Time
		granularity time.Duration
		want        string
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
			got := exprBuilder.TimeFilterBucketAlignedExpr(tt.from, tt.to, tt.granularity)
			assert.Equal(t, tt.want, got)

		})
	}
}

func TestTimeExpressionBuilder_TimeExpr(t *testing.T) {
	testCases := []struct {
		format string
		ts     time.Time
		want   string
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
		t.Run(tt.format, func(t *testing.T) {
			exprBuilder, err := NewTimeExpressionBuilder("time", tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, exprBuilder.TimeExpr(tt.ts))
		})
	}
}

func TestTimeExpressionBuilder_TimeGroupExpr(t *testing.T) {
	testCases := []struct {
		format      string
		granularity string
		want        string
	}{
		{granularity: "1:MINUTES", format: "EPOCH_NANOS", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:NANOSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:NANOSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|NANOSECONDS", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|NANOSECONDS|1", want: `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|NANOSECONDS|2", want: `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_MICROS", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:MICROSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:MICROSECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MICROSECONDS", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MICROSECONDS|1", want: `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MICROSECONDS|2", want: `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_MILLIS", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:MILLISECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:MILLISECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MILLISECONDS", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MILLISECONDS|1", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MILLISECONDS|2", want: `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "TIMESTAMP", want: `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_SECONDS", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:SECONDS:EPOCH", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:SECONDS:EPOCH", want: `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|SECONDS", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|SECONDS|1", want: `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|SECONDS|2", want: `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_MINUTES", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:MINUTES:EPOCH", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:MINUTES:EPOCH", want: `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MINUTES", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MINUTES|1", want: `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|MINUTES|2", want: `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_HOURS", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:HOURS:EPOCH", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:HOURS:EPOCH", want: `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|HOURS", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|HOURS|1", want: `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|HOURS|2", want: `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH_DAYS", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "1:DAYS:EPOCH", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "2:DAYS:EPOCH", want: `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|DAYS", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|DAYS|1", want: `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
		{granularity: "1:MINUTES", format: "EPOCH|DAYS|2", want: `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`},
	}

	for _, tt := range testCases {
		t.Run(tt.format, func(t *testing.T) {
			exprBuilder, err := NewTimeExpressionBuilder("ts", tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, exprBuilder.TimeGroupExpr(tt.granularity))
		})
	}
}
