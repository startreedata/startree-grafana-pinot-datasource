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
		{
			format: "EPOCH_NANOS",
			ts:     time.Unix(3600, 0),
			want:   `3600000000000`,
		}, {
			format: "1:NANOSECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `3600000000000`,
		}, {
			format: "2:NANOSECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `1800000000000`,
		}, {
			format: "EPOCH|NANOSECONDS",
			ts:     time.Unix(3600, 0),
			want:   `3600000000000`,
		}, {
			format: "EPOCH|NANOSECONDS|1",
			ts:     time.Unix(3600, 0),
			want:   `3600000000000`,
		}, {
			format: "EPOCH|NANOSECONDS|2",
			ts:     time.Unix(3600, 0),
			want:   `1800000000000`,
		}, {
			format: "EPOCH_MICROS",
			ts:     time.Unix(3600, 0),
			want:   `3600000000`,
		}, {
			format: "1:MICROSECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `3600000000`,
		}, {
			format: "2:MICROSECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `1800000000`,
		}, {
			format: "EPOCH|MICROSECONDS",
			ts:     time.Unix(3600, 0),
			want:   `3600000000`,
		}, {
			format: "EPOCH|MICROSECONDS|1",
			ts:     time.Unix(3600, 0),
			want:   `3600000000`,
		}, {
			format: "EPOCH|MICROSECONDS|2",
			ts:     time.Unix(3600, 0),
			want:   `1800000000`,
		}, {
			format: "EPOCH_MILLIS",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "1:MILLISECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "2:MILLISECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `1800000`,
		}, {
			format: "EPOCH|MILLISECONDS",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "EPOCH|MILLISECONDS|1",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "EPOCH|MILLISECONDS|2",
			ts:     time.Unix(3600, 0),
			want:   `1800000`,
		}, {
			format: "EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "TIMESTAMP",
			ts:     time.Unix(3600, 0),
			want:   `3600000`,
		}, {
			format: "EPOCH_SECONDS",
			ts:     time.Unix(3600, 0),
			want:   `3600`,
		}, {
			format: "1:SECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `3600`,
		}, {
			format: "2:SECONDS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `1800`,
		}, {
			format: "EPOCH|SECONDS",
			ts:     time.Unix(3600, 0),
			want:   `3600`,
		}, {
			format: "EPOCH|SECONDS|1",
			ts:     time.Unix(3600, 0),
			want:   `3600`,
		}, {
			format: "EPOCH|SECONDS|2",
			ts:     time.Unix(3600, 0),
			want:   `1800`,
		}, {
			format: "EPOCH_MINUTES",
			ts:     time.Unix(3600, 0),
			want:   `60`,
		}, {
			format: "1:MINUTES:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `60`,
		}, {
			format: "2:MINUTES:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `30`,
		}, {
			format: "EPOCH|MINUTES",
			ts:     time.Unix(3600, 0),
			want:   `60`,
		}, {
			format: "EPOCH|MINUTES|1",
			ts:     time.Unix(3600, 0),
			want:   `60`,
		}, {
			format: "EPOCH|MINUTES|2",
			ts:     time.Unix(3600, 0),
			want:   `30`,
		}, {
			format: "EPOCH_HOURS",
			ts:     time.Unix(3600, 0),
			want:   `1`,
		}, {
			format: "1:HOURS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `1`,
		}, {
			format: "2:HOURS:EPOCH",
			ts:     time.Unix(3600, 0),
			want:   `0`,
		}, {
			format: "EPOCH|HOURS",
			ts:     time.Unix(3600, 0),
			want:   `1`,
		}, {
			format: "EPOCH|HOURS|1",
			ts:     time.Unix(3600, 0),
			want:   `1`,
		}, {
			format: "EPOCH|HOURS|2",
			ts:     time.Unix(3600, 0),
			want:   `0`,
		}, {
			format: "EPOCH_DAYS",
			ts:     time.Unix(24*5*3600, 0),
			want:   `5`,
		}, {
			format: "1:DAYS:EPOCH",
			ts:     time.Unix(24*5*3600, 0),
			want:   `5`,
		}, {
			format: "2:DAYS:EPOCH",
			ts:     time.Unix(24*5*3600, 0),
			want:   `2`,
		}, {
			format: "EPOCH|DAYS",
			ts:     time.Unix(24*5*3600, 0),
			want:   `5`,
		}, {
			format: "EPOCH|DAYS|1",
			ts:     time.Unix(24*5*3600, 0),
			want:   `5`,
		}, {
			format: "EPOCH|DAYS|2",
			ts:     time.Unix(24*5*3600, 0),
			want:   `2`,
		},
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
		{
			format:      "EPOCH_NANOS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:NANOSECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:NANOSECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|NANOSECONDS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|NANOSECONDS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|NANOSECONDS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:NANOSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_MICROS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:MICROSECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:MICROSECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MICROSECONDS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MICROSECONDS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MICROSECONDS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MICROSECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_MILLIS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:MILLISECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:MILLISECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MILLISECONDS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MILLISECONDS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MILLISECONDS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "TIMESTAMP",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_SECONDS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:SECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:SECONDS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|SECONDS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|SECONDS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|SECONDS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_MINUTES",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:MINUTES:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:MINUTES:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MINUTES",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MINUTES|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|MINUTES|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:MINUTES:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_HOURS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:HOURS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:HOURS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|HOURS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|HOURS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|HOURS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:HOURS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH_DAYS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "1:DAYS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "2:DAYS:EPOCH",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|DAYS",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|DAYS|1",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		}, {
			format:      "EPOCH|DAYS|2",
			granularity: "1:MINUTES",
			want:        `DATETIMECONVERT("ts", '2:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.format, func(t *testing.T) {
			exprBuilder, err := NewTimeExpressionBuilder("ts", tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, exprBuilder.TimeGroupExpr(tt.granularity))
		})
	}
}
