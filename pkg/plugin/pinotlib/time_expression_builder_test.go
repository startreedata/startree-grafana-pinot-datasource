package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

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

func TestEquivalentBucketExpressions(t *testing.T) {
	testArgs := []struct {
		expr1, expr2 string
		col          string
		want         bool
	}{
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 1), 1)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '2:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 2), 2)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '5:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 5), 5)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '10:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 10), 10)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '15:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 15), 15)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '30:MINUTES')`,
			expr2: `FromEpochMinutesBucket(ToEpochMinutesBucket("zz_received_timestamp", 30), 30)`,
			col:   "zz_received_timestamp",
			want:  true,
		},

		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')`,
			expr2: `FromEpochHoursBucket(ToEpochHoursBucket("zz_received_timestamp", 1), 1)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
		{
			expr1: `DATETIMECONVERT("zz_received_timestamp", '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS')`,
			expr2: `FromEpochDaysBucket(ToEpochDaysBucket("zz_received_timestamp", 1), 1)`,
			col:   "zz_received_timestamp",
			want:  true,
		},
	}

	// The general approach is this:
	// Parse the conversion expr into a DateTimeBucketConversion.
	// DateTimeConvert → this is basically just parse the args.
	// FromEpochBucket(ToEpochBucket()) → input&output is always milliseconds epoch.
	//   Granularity is determined by the function name and int arg.
	//   Granularity should be the same for FromEpochBucket & ToEpochBucket.

	type DateTimeBucketConversion struct {
		col          string
		inputFormat  string
		outputFormat string
		granularity  PinotGranularity
	}
	const DateTimeConvertFunction = "DATETIMECONVERT"

	getDateTimeBucketConversion := func(expr string) (DateTimeBucketConversion, bool) {
		expr = strings.TrimSpace(expr)

		dateTimeConvertRegex := regexp.MustCompile(`(?i)^DATETIMECONVERT\s*\(\s*(\S+)\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*,\s*'(\S+)'\s*\)$`)
		epochBucketRegex := regexp.MustCompile(`(?i)^FromEpoch(\w+)Bucket\s*\(\s*ToEpoch(\w+)Bucket\s*\(\s*(\S+)\s*,\s*(\d+)\s*\)\s*,\s*(\d+)\s*\)$`)

		matchAndArgs := dateTimeConvertRegex.FindStringSubmatch(expr)
		if len(matchAndArgs) == 5 {
			granularity, err := ParsePinotGranularity(matchAndArgs[4])
			if err != nil {
				return DateTimeBucketConversion{}, false
			}
			return DateTimeBucketConversion{
				col:          UnquoteObjectName(matchAndArgs[1]),
				inputFormat:  matchAndArgs[2],
				outputFormat: matchAndArgs[3],
				granularity:  granularity,
			}, true
		}

		matchAndArgs = epochBucketRegex.FindStringSubmatch(expr)
		if len(matchAndArgs) == 6 {
			fromUnit := strings.ToUpper(matchAndArgs[1])
			toUnit := strings.ToUpper(matchAndArgs[2])
			col := UnquoteObjectName(matchAndArgs[3])
			toSize := matchAndArgs[4]
			fromSize := matchAndArgs[5]

			if fromUnit != toUnit || toSize != fromSize {
				return DateTimeBucketConversion{}, false
			}

			size, err := strconv.ParseUint(toSize, 10, 64)
			if err != nil {
				return DateTimeBucketConversion{}, false
			}
			granularity, err := NewPinotGranularity(fromUnit, uint(size))
			if err != nil {
				return DateTimeBucketConversion{}, false
			}

			return DateTimeBucketConversion{
				col:          col,
				inputFormat:  "1:MILLISECONDS:EPOCH",
				outputFormat: "1:MILLISECONDS:EPOCH",
				granularity:  granularity,
			}, true
		}
		return DateTimeBucketConversion{}, false
	}

	convsAreEqual := func(conv1, conv2 DateTimeBucketConversion) bool {
		return conv1.col == conv2.col &&
			conv1.inputFormat == conv2.inputFormat &&
			conv1.outputFormat == conv2.outputFormat &&
			conv1.granularity.Duration() == conv2.granularity.Duration()
	}

	for _, tt := range testArgs {
		t.Run(tt.expr1, func(t *testing.T) {
			conv1, ok := getDateTimeBucketConversion(tt.expr1)
			assert.True(t, ok, tt.expr1)
			conv2, ok := getDateTimeBucketConversion(tt.expr2)
			assert.True(t, ok, tt.expr2)
			assert.Equal(t, tt.want, convsAreEqual(conv1, conv2))
		})
	}
}
