package pinot

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestParseTimeUnit(t *testing.T) {
	tests := []struct {
		unit    string
		want    TimeUnit
		wantErr error
	}{
		{unit: "NANOSECONDS", want: TimeUnitNanoseconds},
		{unit: "nanoseconds", want: TimeUnitNanoseconds},
		{unit: "MICROSECONDS", want: TimeUnitMicroseconds},
		{unit: "microseconds", want: TimeUnitMicroseconds},
		{unit: "MILLISECONDS", want: TimeUnitMilliseconds},
		{unit: "milliseconds", want: TimeUnitMilliseconds},
		{unit: "SECONDS", want: TimeUnitSeconds},
		{unit: "seconds", want: TimeUnitSeconds},
		{unit: "MINUTES", want: TimeUnitMinutes},
		{unit: "minutes", want: TimeUnitMinutes},
		{unit: "HOURS", want: TimeUnitHours},
		{unit: "hours", want: TimeUnitHours},
		{unit: "DAYS", want: TimeUnitDays},
		{unit: "days", want: TimeUnitDays},
		{unit: "NOT_A_UNIT", wantErr: errors.New("invalid time unit `NOT_A_UNIT`")},
	}
	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			got, err := ParseTimeUnit(tt.unit)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func TestTimeUnit_Duration(t *testing.T) {
	testCases := []struct {
		unit TimeUnit
		want time.Duration
	}{
		{unit: TimeUnitNanoseconds, want: time.Nanosecond},
		{unit: TimeUnitMicroseconds, want: time.Microsecond},
		{unit: TimeUnitMilliseconds, want: time.Millisecond},
		{unit: TimeUnitSeconds, want: time.Second},
		{unit: TimeUnitMinutes, want: time.Minute},
		{unit: TimeUnitHours, want: time.Hour},
		{unit: TimeUnitDays, want: 24 * time.Hour},
		{unit: "NotAUnit", want: 0},
	}
	for _, tt := range testCases {
		t.Run(tt.unit.String(), func(t *testing.T) {
			assert.Equal(t, tt.want, tt.unit.Duration())
		})
	}
}
