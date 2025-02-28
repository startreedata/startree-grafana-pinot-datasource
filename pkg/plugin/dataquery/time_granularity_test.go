package dataquery

import (
	"context"
	"fmt"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestResolveGranularity(t *testing.T) {
	ctx := context.Background()

	format := pinot.DateTimeFormatMillisecondsEpoch()
	derivedGranularities := []pinot.Granularity{
		{Unit: pinot.TimeUnitSeconds, Size: 5},
		{Unit: pinot.TimeUnitSeconds, Size: 15},
		{Unit: pinot.TimeUnitSeconds, Size: 30},
	}

	testCases := []struct {
		expr     string
		fallback time.Duration
		want     string
	}{
		{expr: "", fallback: time.Hour, want: "1:HOURS"},
		{expr: "auto", fallback: time.Microsecond, want: "1:MILLISECONDS"},
		{expr: "auto", fallback: time.Millisecond, want: "1:MILLISECONDS"},
		{expr: "auto", fallback: time.Second, want: "5:SECONDS"},
		{expr: "auto", fallback: 10 * time.Second, want: "15:SECONDS"},
		{expr: "auto", fallback: time.Hour, want: "1:HOURS"},
		{expr: "1:MINUTES", fallback: time.Hour, want: "1:MINUTES"},
		{expr: "GIBBERISH", fallback: time.Hour, want: "1:HOURS"},
	}
	for _, tt := range testCases {
		t.Run(fmt.Sprintf("expr=`%s`,fallback=`%s`", tt.expr, tt.fallback), func(t *testing.T) {
			got := ResolveGranularity(ctx, tt.expr, format, tt.fallback, derivedGranularities)
			assert.Equal(t, tt.want, got.String())
		})
	}
}
