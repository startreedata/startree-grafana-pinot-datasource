package dataquery

import (
	"context"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
	"time"
)

func TestNewDriver(t *testing.T) {
	client := test_helpers.SetupPinotAndCreateClient(t)

	t.Run("hide=true", func(t *testing.T) {
		query := PinotDataQuery{Hide: true}
		got, err := NewDriver(client, query, backend.TimeRange{})
		require.NoError(t, err)
		assert.IsType(t, &NoOpDriver{}, got)
	})

	t.Run("queryType="+string(QueryTypePinotQl), func(t *testing.T) {
		t.Run("editorMode="+string(EditorModeBuilder), func(t *testing.T) {
			query := PinotDataQuery{
				QueryType:           QueryTypePinotQl,
				EditorMode:          EditorModeBuilder,
				TableName:           "benchmark",
				TimeColumn:          "ts",
				MetricColumn:        "value",
				AggregationFunction: "SUM",
				IntervalSize:        1 * time.Second,
			}
			got, err := NewDriver(client, query, backend.TimeRange{})
			assert.NoError(t, err)
			assert.IsType(t, &PinotQlBuilderDriver{}, got)
		})

		t.Run("editorMode="+string(EditorModeCode), func(t *testing.T) {
			query := PinotDataQuery{
				QueryType:    QueryTypePinotQl,
				EditorMode:   EditorModeCode,
				TableName:    "benchmark",
				PinotQlCode:  `select 1;`,
				IntervalSize: 1 * time.Second,
			}
			got, err := NewDriver(client, query, backend.TimeRange{})
			assert.NoError(t, err)
			assert.IsType(t, &PinotQlCodeDriver{}, got)
		})
	})

	t.Run("queryType="+string(QueryTypePromQl), func(t *testing.T) {
		query := PinotDataQuery{
			QueryType: QueryTypePromQl,
		}
		got, err := NewDriver(client, query, backend.TimeRange{})
		assert.NoError(t, err)
		assert.IsType(t, &PromQlDriver{}, got)
	})

	t.Run("queryType="+string(QueryTypePinotVariableQuery), func(t *testing.T) {
		query := PinotDataQuery{
			QueryType: QueryTypePinotVariableQuery,
		}
		got, err := NewDriver(client, query, backend.TimeRange{})
		assert.NoError(t, err)
		assert.IsType(t, &PinotVariableQueryDriver{}, got)
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

func TestNoOpDriver_Execute(t *testing.T) {
	var driver NoOpDriver
	got := driver.Execute(context.Background())
	assert.Equal(t, backend.StatusOK, got.Status)
	assert.Equal(t, data.Frames(nil), got.Frames)
	assert.NoError(t, got.Error)
	assert.Empty(t, 0, got.ErrorSource)
}
