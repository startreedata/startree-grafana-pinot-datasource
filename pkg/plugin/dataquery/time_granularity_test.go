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
