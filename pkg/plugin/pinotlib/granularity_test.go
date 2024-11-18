package pinotlib

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParsePinotGranularity(t *testing.T) {
	tests := []struct {
		granularity string
		want        PinotGranularity
		wantErr     bool
	}{
		{
			granularity: "1:NANOSECONDS",
			want:        PinotGranularity{Unit: TimeUnitNanoseconds, Size: 1},
		},
		{
			granularity: "2:MICROSECONDS",
			want:        PinotGranularity{Unit: TimeUnitMicroseconds, Size: 2},
		},
		{
			granularity: "3:MILLISECONDS",
			want:        PinotGranularity{Unit: TimeUnitMilliseconds, Size: 3},
		},
		{
			granularity: "4:SECONDS",
			want:        PinotGranularity{Unit: TimeUnitSeconds, Size: 4},
		},
		{
			granularity: "5:MINUTES",
			want:        PinotGranularity{Unit: TimeUnitMinutes, Size: 5},
		},
		{
			granularity: "6:HOURS",
			want:        PinotGranularity{Unit: TimeUnitHours, Size: 6},
		},
		{
			granularity: "7:DAYS",
			want:        PinotGranularity{Unit: TimeUnitDays, Size: 7},
		},
		{
			granularity: "1:NotAUnit",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run("granularity="+tt.granularity, func(t *testing.T) {
			got, err := ParsePinotGranularity(tt.granularity)
			assert.Equal(t, tt.want, got)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPinotGranularity_String(t *testing.T) {
	tests := []struct {
		granularity string
		want        string
	}{
		{granularity: "1:NANOSECONDS", want: "1:NANOSECONDS"},
		{granularity: "2:MICROSECONDS", want: "2:MICROSECONDS"},
		{granularity: "3:MILLISECONDS", want: "3:MILLISECONDS"},
		{granularity: "4:SECONDS", want: "4:SECONDS"},
		{granularity: "5:MINUTES", want: "5:MINUTES"},
		{granularity: "6:HOURS", want: "6:HOURS"},
		{granularity: "7:DAYS", want: "7:DAYS"},
	}

	for _, tt := range tests {
		t.Run("granularity="+tt.granularity, func(t *testing.T) {
			got, err := ParsePinotGranularity(tt.granularity)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestPinotGranularity_Duration(t *testing.T) {
	tests := []struct {
		granularity string
		want        time.Duration
	}{
		{granularity: "1:NANOSECONDS", want: time.Nanosecond},
		{granularity: "2:MICROSECONDS", want: 2 * time.Microsecond},
		{granularity: "3:MILLISECONDS", want: 3 * time.Millisecond},
		{granularity: "4:SECONDS", want: 4 * time.Second},
		{granularity: "5:MINUTES", want: 5 * time.Minute},
		{granularity: "6:HOURS", want: 6 * time.Hour},
		{granularity: "7:DAYS", want: 7 * 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run("granularity="+tt.granularity, func(t *testing.T) {
			got, err := ParsePinotGranularity(tt.granularity)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Duration())
		})
	}
}

func TestPinotGranularity_Equals(t *testing.T) {
	tests := []struct {
		expr1, expr2 string
		want         bool
	}{
		{expr1: "1000:NANOSECONDS", expr2: "1:MICROSECONDS", want: true},
		{expr1: "1000000:NANOSECONDS", expr2: "1:MILLISECONDS", want: true},
		{expr1: "1000000000:NANOSECONDS", expr2: "1:SECONDS", want: true},
		{expr1: "1000:MICROSECONDS", expr2: "1:MILLISECONDS", want: true},
		{expr1: "1000000:MICROSECONDS", expr2: "1:SECONDS", want: true},
		{expr1: "1000:MILLISECONDS", expr2: "1:SECONDS", want: true},
		{expr1: "60000:MILLISECONDS", expr2: "1:MINUTES", want: true},
		{expr1: "60:SECONDS", expr2: "1:MINUTES", want: true},
		{expr1: "3600:SECONDS", expr2: "1:HOURS", want: true},
		{expr1: "60:MINUTES", expr2: "1:HOURS", want: true},
		{expr1: "24:HOURS", expr2: "1:DAYS", want: true},
		{expr1: "1001:NANOSECONDS", expr2: "1:MICROSECONDS", want: false},
		{expr1: "1000001:NANOSECONDS", expr2: "1:MILLISECONDS", want: false},
		{expr1: "1000000001:NANOSECONDS", expr2: "1:SECONDS", want: false},
		{expr1: "1001:MICROSECONDS", expr2: "1:MILLISECONDS", want: false},
		{expr1: "1000001:MICROSECONDS", expr2: "1:SECONDS", want: false},
		{expr1: "1001:MILLISECONDS", expr2: "1:SECONDS", want: false},
		{expr1: "60001:MILLISECONDS", expr2: "1:MINUTES", want: false},
		{expr1: "61:SECONDS", expr2: "1:MINUTES", want: false},
		{expr1: "3601:SECONDS", expr2: "1:HOURS", want: false},
		{expr1: "61:MINUTES", expr2: "1:HOURS", want: false},
		{expr1: "25:HOURS", expr2: "1:DAYS", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.expr1+"="+tt.expr2, func(t *testing.T) {
			g1, err := ParsePinotGranularity(tt.expr1)
			require.NoError(t, err)
			g2, err := ParsePinotGranularity(tt.expr2)
			require.NoError(t, err)
			assert.Equal(t, tt.want, g1.Equals(g2))
		})
	}
}
