package pinotlib

import (
	"fmt"
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
	assert.Equal(t, `"time" >= 1 AND "time" < 3601`, got)
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

func TestTimeGroupExpr(t *testing.T) {
	const outputFormat = "1:MILLISECONDS:EPOCH"
	tableConfig := TableConfig{}

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
		t.Run(fmt.Sprintf("format=%s,granularity=%s", tt.format, tt.granularity), func(t *testing.T) {
			group, err := DateTimeConversionOf("ts", tt.format, outputFormat, tt.granularity)
			require.NoError(t, err)
			assert.Equal(t, tt.want, TimeGroupExpr(tableConfig, group))
		})
	}
}
