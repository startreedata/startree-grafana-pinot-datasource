package resources

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseIntervalSize(t *testing.T) {
	testCases := []struct {
		in   string
		want time.Duration
	}{
		// Native time.ParseDuration units.
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"500ms", 500 * time.Millisecond},
		// Grafana day/year units (from secondsToHms) that time.ParseDuration rejects.
		{"1d", 24 * time.Hour},
		{"2d", 2 * 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"1y", 365 * 24 * time.Hour},
		// Unparseable input falls back to 0.
		{"", 0},
		{"garbage", 0},
	}
	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, parseIntervalSize(tt.in))
		})
	}
}
