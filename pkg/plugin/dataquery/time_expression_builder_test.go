package dataquery

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	got := exprBuilder.TimeFilterExpr(TimeRange{From: time.Unix(1, 0), To: time.Unix(3601, 0)})
	assert.Equal(t, `"time" >= 1 AND "time" <= 3601`, got)
}

func TestTimeExpressionBuilder_TimeFilterBucketAlignedExpr(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	require.NoError(t, err)

	got := exprBuilder.TimeFilterBucketAlignedExpr(TimeRange{From: time.Unix(1, 0), To: time.Unix(3601, 0)}, time.Minute)
	assert.Equal(t, `"time" >= 0 AND "time" <= 3660`, got)
}

func TestTimeExpressionBuilder_TimeExpr(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	require.NoError(t, err)

	got := exprBuilder.TimeExpr(time.Unix(3600, 0))
	assert.Equal(t, `3600`, got)
}

func TestTimeExpressionBuilder_TimeGroupExpr(t *testing.T) {
	exprBuilder, err := NewTimeExpressionBuilder("time", "1:SECONDS:EPOCH")
	require.NoError(t, err)

	got := exprBuilder.TimeGroupExpr("1:MINUTES")
	assert.Equal(t, `DATETIMECONVERT("time", '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MINUTES')`, got)
}
