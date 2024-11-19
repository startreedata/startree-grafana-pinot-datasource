package dataquery

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestResolveGranularity(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		expr     string
		fallback time.Duration
		want     string
	}{
		{expr: "", fallback: time.Hour, want: "1:HOURS"},
		{expr: "auto", fallback: time.Hour, want: "1:HOURS"},
		{expr: "1:MINUTES", fallback: time.Hour, want: "1:MINUTES"},
		{expr: "GIBBERISH", fallback: time.Hour, want: "1:HOURS"},
	}
	for _, tt := range testCases {
		t.Run(fmt.Sprintf("expr=`%s`", tt.expr), func(t *testing.T) {
			got := ResolveGranularity(ctx, tt.expr, tt.fallback)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

func TestParseGranularityExpr(t *testing.T) {
	testArgs := []struct {
		expr string
		want time.Duration
	}{
		{expr: "NANOSECONDS", want: time.Nanosecond},
		{expr: "7:NANOSECONDS", want: 7 * time.Nanosecond},
		{expr: "MICROSECONDS", want: time.Microsecond},
		{expr: "7:MICROSECONDS", want: 7 * time.Microsecond},
		{expr: "MILLISECONDS", want: time.Millisecond},
		{expr: "7:MILLISECONDS", want: 7 * time.Millisecond},
		{expr: "SECONDS", want: time.Second},
		{expr: "7:SECONDS", want: 7 * time.Second},
		{expr: "MINUTES", want: time.Minute},
		{expr: "7:MINUTES", want: 7 * time.Minute},
		{expr: "HOURS", want: time.Hour},
		{expr: "7:HOURS", want: 7 * time.Hour},
		{expr: "DAYS", want: 24 * time.Hour},
		{expr: "7:DAYS", want: 7 * 24 * time.Hour},
	}
	for _, tt := range testArgs {
		t.Run(tt.expr, func(t *testing.T) {
			got, err := ParseGranularityExpr(tt.expr)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("GIBBERISH", func(t *testing.T) {
		got, err := ParseGranularityExpr("GIBBERISH")
		assert.Error(t, err)
		assert.Equal(t, time.Duration(0), got)
	})
}

func TestGranularityExprFrom(t *testing.T) {
	testArgs := []struct {
		size time.Duration
		want string
	}{
		{size: 7 * time.Nanosecond, want: "7:NANOSECONDS"},
		{size: 7 * time.Microsecond, want: "7:MICROSECONDS"},
		{size: 7 * time.Millisecond, want: "7:MILLISECONDS"},
		{size: 7 * time.Second, want: "7:SECONDS"},
		{size: 7 * time.Minute, want: "7:MINUTES"},
		{size: 7 * time.Hour, want: "7:HOURS"},
		{size: 27 * time.Hour, want: "27:HOURS"},
	}
	for _, tt := range testArgs {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, GranularityExprFrom(tt.size))
		})
	}
}
