package dataquery

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestNewDriver(t *testing.T) {
	t.Run("hidden", func(t *testing.T) {
		query := PinotDataQuery{Hide: true}
		got, err := NewDriver(nil, query, backend.TimeRange{})
		require.NoError(t, err)
		assert.IsType(t, &NoOpDriver{}, got)
	})
}

func sliceToPointers[V any](arr []V) []*V {
	res := make([]*V, len(arr))
	for i := range arr {
		res[i] = &arr[i]
	}
	return res
}

func assertBrokerExceptionErrorWithCodes(t *testing.T, err error, codes ...int) {
	var brokerError *pinotlib.BrokerExceptionError
	if assert.ErrorAs(t, err, &brokerError) {
		assert.NotEmpty(t, brokerError.Exceptions)
		var exceptionCodes []int
		for _, exception := range brokerError.Exceptions {
			exceptionCodes = append(exceptionCodes, exception.ErrorCode)
		}
		sort.Ints(exceptionCodes)
		sort.Ints(codes)
		assert.Equal(t, codes, exceptionCodes, "exception codes")
	}
}
