package plugin

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimeExpressionBuilder_GranularityExpr(t *testing.T) {
	tests := []struct {
		bucketSize time.Duration
		want       string
	}{
		{bucketSize: 75 * time.Hour, want: "75:HOURS"},
		{bucketSize: 3 * time.Hour, want: "3:HOURS"},
		{bucketSize: 5 * time.Minute, want: "5:MINUTES"},
		{bucketSize: 1 * time.Second, want: "1:SECONDS"},
		{bucketSize: 1 * time.Millisecond, want: "1:MILLISECONDS"},
		{bucketSize: 1 * time.Microsecond, want: "1:MICROSECONDS"},
		{bucketSize: 1 * time.Nanosecond, want: "1:NANOSECONDS"},
	}

	builder := TimeExpressionBuilder{}
	for _, tt := range tests {
		t.Run(tt.bucketSize.String(), func(t *testing.T) {
			got := builder.GranularityExpr(tt.bucketSize)
			assert.Equal(t, tt.want, got)
		})
	}
}
