package dataquery

import (
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

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
