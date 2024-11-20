package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseDateTimeFormat(t *testing.T) {
	testCases := []struct {
		format  string
		want    DateTimeFormat
		wantErr error
	}{
		{format: "EPOCH_NANOS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}},
		{format: "1:NANOSECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}},
		{format: "2:NANOSECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitNanoseconds}},
		{format: "EPOCH|NANOSECONDS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}},
		{format: "EPOCH|NANOSECONDS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitNanoseconds}},
		{format: "EPOCH|NANOSECONDS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitNanoseconds}},
		{format: "EPOCH_MICROS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}},
		{format: "1:MICROSECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}},
		{format: "2:MICROSECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMicroseconds}},
		{format: "EPOCH|MICROSECONDS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}},
		{format: "EPOCH|MICROSECONDS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMicroseconds}},
		{format: "EPOCH|MICROSECONDS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMicroseconds}},
		{format: "EPOCH_MILLIS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "1:MILLISECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "2:MILLISECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMilliseconds}},
		{format: "EPOCH|MILLISECONDS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "EPOCH|MILLISECONDS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "EPOCH|MILLISECONDS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMilliseconds}},
		{format: "EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "TIMESTAMP", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMilliseconds}},
		{format: "EPOCH_SECONDS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}},
		{format: "1:SECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}},
		{format: "2:SECONDS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitSeconds}},
		{format: "EPOCH|SECONDS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}},
		{format: "EPOCH|SECONDS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitSeconds}},
		{format: "EPOCH|SECONDS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitSeconds}},
		{format: "EPOCH_MINUTES", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}},
		{format: "1:MINUTES:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}},
		{format: "2:MINUTES:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMinutes}},
		{format: "EPOCH|MINUTES", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}},
		{format: "EPOCH|MINUTES|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitMinutes}},
		{format: "EPOCH|MINUTES|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitMinutes}},
		{format: "EPOCH_HOURS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}},
		{format: "1:HOURS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}},
		{format: "2:HOURS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitHours}},
		{format: "EPOCH|HOURS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}},
		{format: "EPOCH|HOURS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitHours}},
		{format: "EPOCH|HOURS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitHours}},
		{format: "EPOCH_DAYS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}},
		{format: "1:DAYS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}},
		{format: "2:DAYS:EPOCH", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitDays}},
		{format: "EPOCH|DAYS", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}},
		{format: "EPOCH|DAYS|1", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 1, Unit: TimeUnitDays}},
		{format: "EPOCH|DAYS|2", want: DateTimeFormat{Format: TimeFormatEpoch, Size: 2, Unit: TimeUnitDays}},
	}

	for _, tt := range testCases {
		t.Run("format="+tt.format, func(t *testing.T) {
			got, err := ParseDateTimeFormat(tt.format)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func TestDateTimeFormat_LegacyString(t *testing.T) {
	testCases := []struct {
		format string
		ts     time.Time
		want   string
	}{
		{format: "EPOCH_NANOS", want: "1:NANOSECONDS:EPOCH"},
		{format: "1:NANOSECONDS:EPOCH", want: "1:NANOSECONDS:EPOCH"},
		{format: "2:NANOSECONDS:EPOCH", want: "2:NANOSECONDS:EPOCH"},
		{format: "EPOCH|NANOSECONDS", want: "1:NANOSECONDS:EPOCH"},
		{format: "EPOCH|NANOSECONDS|1", want: "1:NANOSECONDS:EPOCH"},
		{format: "EPOCH|NANOSECONDS|2", want: "2:NANOSECONDS:EPOCH"},
		{format: "EPOCH_MICROS", want: "1:MICROSECONDS:EPOCH"},
		{format: "1:MICROSECONDS:EPOCH", want: "1:MICROSECONDS:EPOCH"},
		{format: "2:MICROSECONDS:EPOCH", want: "2:MICROSECONDS:EPOCH"},
		{format: "EPOCH|MICROSECONDS", want: "1:MICROSECONDS:EPOCH"},
		{format: "EPOCH|MICROSECONDS|1", want: "1:MICROSECONDS:EPOCH"},
		{format: "EPOCH|MICROSECONDS|2", want: "2:MICROSECONDS:EPOCH"},
		{format: "EPOCH_MILLIS", want: "1:MILLISECONDS:EPOCH"},
		{format: "1:MILLISECONDS:EPOCH", want: "1:MILLISECONDS:EPOCH"},
		{format: "2:MILLISECONDS:EPOCH", want: "2:MILLISECONDS:EPOCH"},
		{format: "EPOCH|MILLISECONDS", want: "1:MILLISECONDS:EPOCH"},
		{format: "EPOCH|MILLISECONDS|1", want: "1:MILLISECONDS:EPOCH"},
		{format: "EPOCH|MILLISECONDS|2", want: "2:MILLISECONDS:EPOCH"},
		{format: "EPOCH", want: "1:MILLISECONDS:EPOCH"},
		{format: "TIMESTAMP", want: "1:MILLISECONDS:EPOCH"},
		{format: "EPOCH_SECONDS", want: "1:SECONDS:EPOCH"},
		{format: "1:SECONDS:EPOCH", want: "1:SECONDS:EPOCH"},
		{format: "2:SECONDS:EPOCH", want: "2:SECONDS:EPOCH"},
		{format: "EPOCH|SECONDS", want: "1:SECONDS:EPOCH"},
		{format: "EPOCH|SECONDS|1", want: "1:SECONDS:EPOCH"},
		{format: "EPOCH|SECONDS|2", want: "2:SECONDS:EPOCH"},
		{format: "EPOCH_MINUTES", want: "1:MINUTES:EPOCH"},
		{format: "1:MINUTES:EPOCH", want: "1:MINUTES:EPOCH"},
		{format: "2:MINUTES:EPOCH", want: "2:MINUTES:EPOCH"},
		{format: "EPOCH|MINUTES", want: "1:MINUTES:EPOCH"},
		{format: "EPOCH|MINUTES|1", want: "1:MINUTES:EPOCH"},
		{format: "EPOCH|MINUTES|2", want: "2:MINUTES:EPOCH"},
		{format: "EPOCH_HOURS", want: "1:HOURS:EPOCH"},
		{format: "1:HOURS:EPOCH", want: "1:HOURS:EPOCH"},
		{format: "2:HOURS:EPOCH", want: "2:HOURS:EPOCH"},
		{format: "EPOCH|HOURS", want: "1:HOURS:EPOCH"},
		{format: "EPOCH|HOURS|1", want: "1:HOURS:EPOCH"},
		{format: "EPOCH|HOURS|2", want: "2:HOURS:EPOCH"},
		{format: "EPOCH_DAYS", want: "1:DAYS:EPOCH"},
		{format: "1:DAYS:EPOCH", want: "1:DAYS:EPOCH"},
		{format: "2:DAYS:EPOCH", want: "2:DAYS:EPOCH"},
		{format: "EPOCH|DAYS", want: "1:DAYS:EPOCH"},
		{format: "EPOCH|DAYS|1", want: "1:DAYS:EPOCH"},
		{format: "EPOCH|DAYS|2", want: "2:DAYS:EPOCH"},
	}

	for _, tt := range testCases {
		t.Run("format="+tt.format, func(t *testing.T) {
			format, err := ParseDateTimeFormat(tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, format.LegacyString())
		})
	}
}

func TestDateTimeFormat_V0_12String(t *testing.T) {
	testCases := []struct {
		format string
		ts     time.Time
		want   string
	}{
		{format: "EPOCH_NANOS", want: "EPOCH|NANOSECONDS|1"},
		{format: "1:NANOSECONDS:EPOCH", want: "EPOCH|NANOSECONDS|1"},
		{format: "2:NANOSECONDS:EPOCH", want: "EPOCH|NANOSECONDS|2"},
		{format: "EPOCH|NANOSECONDS", want: "EPOCH|NANOSECONDS|1"},
		{format: "EPOCH|NANOSECONDS|1", want: "EPOCH|NANOSECONDS|1"},
		{format: "EPOCH|NANOSECONDS|2", want: "EPOCH|NANOSECONDS|2"},
		{format: "EPOCH_MICROS", want: "EPOCH|MICROSECONDS|1"},
		{format: "1:MICROSECONDS:EPOCH", want: "EPOCH|MICROSECONDS|1"},
		{format: "2:MICROSECONDS:EPOCH", want: "EPOCH|MICROSECONDS|2"},
		{format: "EPOCH|MICROSECONDS", want: "EPOCH|MICROSECONDS|1"},
		{format: "EPOCH|MICROSECONDS|1", want: "EPOCH|MICROSECONDS|1"},
		{format: "EPOCH|MICROSECONDS|2", want: "EPOCH|MICROSECONDS|2"},
		{format: "EPOCH_MILLIS", want: "EPOCH|MILLISECONDS|1"},
		{format: "1:MILLISECONDS:EPOCH", want: "EPOCH|MILLISECONDS|1"},
		{format: "2:MILLISECONDS:EPOCH", want: "EPOCH|MILLISECONDS|2"},
		{format: "EPOCH|MILLISECONDS", want: "EPOCH|MILLISECONDS|1"},
		{format: "EPOCH|MILLISECONDS|1", want: "EPOCH|MILLISECONDS|1"},
		{format: "EPOCH|MILLISECONDS|2", want: "EPOCH|MILLISECONDS|2"},
		{format: "EPOCH", want: "EPOCH|MILLISECONDS|1"},
		{format: "TIMESTAMP", want: "EPOCH|MILLISECONDS|1"},
		{format: "EPOCH_SECONDS", want: "EPOCH|SECONDS|1"},
		{format: "1:SECONDS:EPOCH", want: "EPOCH|SECONDS|1"},
		{format: "2:SECONDS:EPOCH", want: "EPOCH|SECONDS|2"},
		{format: "EPOCH|SECONDS", want: "EPOCH|SECONDS|1"},
		{format: "EPOCH|SECONDS|1", want: "EPOCH|SECONDS|1"},
		{format: "EPOCH|SECONDS|2", want: "EPOCH|SECONDS|2"},
		{format: "EPOCH_MINUTES", want: "EPOCH|MINUTES|1"},
		{format: "1:MINUTES:EPOCH", want: "EPOCH|MINUTES|1"},
		{format: "2:MINUTES:EPOCH", want: "EPOCH|MINUTES|2"},
		{format: "EPOCH|MINUTES", want: "EPOCH|MINUTES|1"},
		{format: "EPOCH|MINUTES|1", want: "EPOCH|MINUTES|1"},
		{format: "EPOCH|MINUTES|2", want: "EPOCH|MINUTES|2"},
		{format: "EPOCH_HOURS", want: "EPOCH|HOURS|1"},
		{format: "1:HOURS:EPOCH", want: "EPOCH|HOURS|1"},
		{format: "2:HOURS:EPOCH", want: "EPOCH|HOURS|2"},
		{format: "EPOCH|HOURS", want: "EPOCH|HOURS|1"},
		{format: "EPOCH|HOURS|1", want: "EPOCH|HOURS|1"},
		{format: "EPOCH|HOURS|2", want: "EPOCH|HOURS|2"},
		{format: "EPOCH_DAYS", want: "EPOCH|DAYS|1"},
		{format: "1:DAYS:EPOCH", want: "EPOCH|DAYS|1"},
		{format: "2:DAYS:EPOCH", want: "EPOCH|DAYS|2"},
		{format: "EPOCH|DAYS", want: "EPOCH|DAYS|1"},
		{format: "EPOCH|DAYS|1", want: "EPOCH|DAYS|1"},
		{format: "EPOCH|DAYS|2", want: "EPOCH|DAYS|2"},
	}

	for _, tt := range testCases {
		t.Run("format="+tt.format, func(t *testing.T) {
			format, err := ParseDateTimeFormat(tt.format)
			require.NoError(t, err)
			assert.Equal(t, tt.want, format.V0_12String())
		})
	}
}
