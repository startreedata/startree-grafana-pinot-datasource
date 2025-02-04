package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"sort"
	"testing"
	"time"
)

func TestExecutableQueryFrom(t *testing.T) {
	timeRange := TimeRange{
		From: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	t.Run("hide=true", func(t *testing.T) {
		got := ExecutableQueryFrom(DataQuery{Hide: true})
		assert.IsType(t, &NoOpQuery{}, got)
	})

	t.Run("queryType="+string(QueryTypePinotQl), func(t *testing.T) {
		t.Run("editorMode="+string(EditorModeBuilder), func(t *testing.T) {
			got := ExecutableQueryFrom(DataQuery{
				TimeRange:           timeRange,
				QueryType:           QueryTypePinotQl,
				EditorMode:          EditorModeBuilder,
				TableName:           "benchmark",
				TimeColumn:          "ts",
				MetricColumn:        "value",
				AggregationFunction: "SUM",
				IntervalSize:        1 * time.Second,
			})
			if assert.IsType(t, TimeSeriesBuilderQuery{}, got) {
				assert.Equal(t, TimeSeriesBuilderQuery{
					TimeRange:           timeRange,
					IntervalSize:        1 * time.Second,
					TableName:           "benchmark",
					TimeColumn:          "ts",
					MetricColumn:        ComplexField{Name: "value"},
					AggregationFunction: "SUM",
					GroupByColumns:      []ComplexField{},
				}, got.(TimeSeriesBuilderQuery))
			}
		})

		t.Run("editorMode="+string(EditorModeCode), func(t *testing.T) {
			got := ExecutableQueryFrom(DataQuery{
				TimeRange:    timeRange,
				QueryType:    QueryTypePinotQl,
				EditorMode:   EditorModeCode,
				TableName:    "benchmark",
				PinotQlCode:  `select 1;`,
				IntervalSize: 1 * time.Second,
			})
			if assert.IsType(t, PinotQlCodeQuery{}, got) {
				assert.Equal(t, PinotQlCodeQuery{
					Code:         `select 1;`,
					TableName:    "benchmark",
					TimeRange:    timeRange,
					IntervalSize: 1 * time.Second,
				}, got.(PinotQlCodeQuery))
			}
		})
	})

	t.Run("queryType="+string(QueryTypePromQl), func(t *testing.T) {
		got := ExecutableQueryFrom(DataQuery{
			QueryType: QueryTypePromQl,
		})
		if assert.IsType(t, PromQlQuery{}, got) {
			assert.Equal(t, PromQlQuery{}, got.(PromQlQuery))
		}
	})

	t.Run("queryType="+string(QueryTypePinotVariableQuery), func(t *testing.T) {
		got := ExecutableQueryFrom(DataQuery{
			QueryType: QueryTypePinotVariableQuery,
		})
		assert.IsType(t, VariableQuery{}, got)
	})
}

func TestNoOpDriver_Execute(t *testing.T) {
	var driver NoOpQuery
	got := driver.Execute(context.Background(), nil)
	assert.Equal(t, backend.StatusOK, got.Status)
	assert.Equal(t, data.Frames(nil), got.Frames)
	assert.NoError(t, got.Error)
	assert.Empty(t, 0, got.ErrorSource)
}

func sliceToPointers[V any](arr []V) []*V {
	res := make([]*V, len(arr))
	for i := range arr {
		res[i] = &arr[i]
	}
	return res
}

func sliceToStrings[V any](arr []V) []string {
	res := make([]string, len(arr))
	for i := range arr {
		res[i] = fmt.Sprintf("%v", arr[i])
	}
	return res
}

func assertBrokerExceptionErrorWithCodes(t *testing.T, err error, codes ...int) {
	t.Helper()
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

type DriverTestCase struct {
	TimeRange    TimeRange
	TableName    string
	TableSchema  pinotlib.TableSchema
	TimeColumn   string
	TargetColumn string
	IntervalSize time.Duration
}

func runSqlQuerySumHappyPath(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery, wantFrames func(times []time.Time, values []float64) data.Frames) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	benchmarkTableSchema, err := client.GetTableSchema(context.Background(), "benchmark")
	require.NoError(t, err)

	got := newDriver(DriverTestCase{
		TableName:    "benchmark",
		TableSchema:  benchmarkTableSchema,
		TargetColumn: "value",
		TimeColumn:   "ts",
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
		},
		IntervalSize: 1 * time.Minute,
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
	assert.Equal(t, wantFrames(
		[]time.Time{
			time.Date(2024, 10, 1, 0, 4, 0, 0, time.UTC),
			time.Date(2024, 10, 1, 0, 3, 0, 0, time.UTC),
			time.Date(2024, 10, 1, 0, 2, 0, 0, time.UTC),
			time.Date(2024, 10, 1, 0, 1, 0, 0, time.UTC),
			time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
		},
		[]float64{
			4.995000894259197e+07,
			4.9950041761314005e+07,
			4.9949916961369045e+07,
			4.994997804782016e+07,
			4.995001567005852e+07,
		},
	), got.Frames, "DataResponse.Frames")
	assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
	assert.NoError(t, got.Error, "DataResponse.Error")

}

func runSqlQuerySumPartialResults(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery, wantFrames func(times []time.Time, values []float64) data.Frames) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	partialTableSchema, err := client.GetTableSchema(context.Background(), "partial")
	require.NoError(t, err)

	got := newDriver(DriverTestCase{
		TableName:    "partial",
		TableSchema:  partialTableSchema,
		TargetColumn: "value",
		TimeColumn:   "ts",
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 2, 0, 5, 0, 0, time.UTC),
		},
		IntervalSize: 1 * time.Minute,
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
	assert.Equal(t, wantFrames(
		[]time.Time{
			time.Date(2024, 10, 2, 0, 4, 0, 0, time.UTC),
			time.Date(2024, 10, 2, 0, 3, 0, 0, time.UTC),
			time.Date(2024, 10, 2, 0, 2, 0, 0, time.UTC),
			time.Date(2024, 10, 2, 0, 1, 0, 0, time.UTC),
			time.Date(2024, 10, 2, 0, 0, 0, 0, time.UTC),
		},
		[]float64{
			603.623178859666,
			598.1350673119193,
			600.9085597026183,
			598.2744783346354,
			601.2399258074636,
		},
	), got.Frames, "DataResponse.Frames")
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
	assertBrokerExceptionErrorWithCodes(t, got.Error, 305)
}

func runSqlQueryDistinctValsHappyPath(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery, wantFrames func(values []string) data.Frames) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	got := newDriver(DriverTestCase{
		TableName:    "infraMetrics",
		TargetColumn: "metric",
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
	assert.Equal(t, wantFrames([]string{
		"db_record_write",
		"http_request_handled",
	}), got.Frames, "DataResponse.Frames")
	assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
	assert.NoError(t, got.Error, "DataResponse.Error")
}

func runSqlQueryDistinctValsPartialResults(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery, wantFrames func(values []string) data.Frames) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	got := newDriver(DriverTestCase{
		TableName:    "partial",
		TargetColumn: "value",
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
	assert.Equal(t, wantFrames(
		[]string{
			"-0.9998874805300421",
			"-0.01791180974867909",
			"-0.0013874257403729828",
			"0.35505207183556975",
			"0.8316667236849353",
			"1.1557837741146118",
			"1.2264435652034988",
			"1.4169743993502841",
			"1.4942925418716824",
			"1.5652983272496814",
			"98.6490549277502",
			"99.30695972033102",
			"99.56307734098407",
			"99.60657322051637",
			"99.90212014753018",
			"100.09896272872888",
			"100.3732127856829",
			"100.7352226606245",
			"101.15030952570145",
			"101.46172209348515",
			"197.88351264832372",
			"199.45711282230633",
			"199.49082509658058",
			"199.69390417034177",
			"199.70431138331926",
			"199.90420255677003",
			"200.1686311770071",
			"200.28951915663532",
			"200.65340736830686",
			"201.3783405915443",
			"298.04830672209266",
			"299.5157294531979",
			"300.1862223943277",
			"300.26483294839056",
			"300.5121667066269",
			"300.56786305369445",
			"300.5953774262084",
			"300.61648234383495",
			"301.0219221033833",
			"301.1795843063259",
		},
	), got.Frames, "DataResponse.Frames")
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
	assertBrokerExceptionErrorWithCodes(t, got.Error, 305)
}

func runSqlQueryNoRows(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	schema, err := client.GetTableSchema(context.Background(), "empty")
	require.NoError(t, err)

	got := newDriver(DriverTestCase{
		TableName:    "empty",
		TableSchema:  schema,
		TargetColumn: "value",
		TimeColumn:   "ts",
		TimeRange: TimeRange{
			From: time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 11, 1, 0, 5, 0, 0, time.UTC),
		},
		IntervalSize: 1 * time.Minute,
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusOK, got.Status, "DataResponse.Status")
	assert.Empty(t, got.Frames, "DataResponse.Frames")
	assert.Empty(t, got.ErrorSource, "DataResponse.ErrorSource")
	assert.NoError(t, got.Error, "DataResponse.Error")
}

func runSqlQueryColumnDne(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	benchmarkTableSchema, err := client.GetTableSchema(context.Background(), "benchmark")
	require.NoError(t, err)

	got := newDriver(DriverTestCase{
		TableName:    "benchmark",
		TableSchema:  benchmarkTableSchema,
		TargetColumn: "not_a_column",
		TimeColumn:   "ts",
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
		},
		IntervalSize: 1 * time.Minute,
	}).Execute(context.Background(), client)
	assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
	assert.Empty(t, got.Frames, "DataResponse.Frames")
	assert.Equal(t, backend.ErrorSourceDownstream, got.ErrorSource, "DataResponse.ErrorSource")
	assertBrokerExceptionErrorWithCodes(t, got.Error, 710)
}

func runSqlQueryPinotUnreachable(t *testing.T, newDriver func(testCase DriverTestCase) ExecutableQuery) {
	t.Helper()
	client := test_helpers.SetupPinotAndCreateClient(t)

	benchmarkTableSchema, err := client.GetTableSchema(context.Background(), "benchmark")
	require.NoError(t, err)

	unreachableClient := pinotlib.NewPinotClient(http.DefaultClient, pinotlib.PinotClientProperties{
		ControllerUrl: "not a url",
		BrokerUrl:     "not a url",
	})

	got := newDriver(DriverTestCase{
		TableName:    "benchmark",
		TableSchema:  benchmarkTableSchema,
		TimeColumn:   "ts",
		TargetColumn: "value",
		TimeRange: TimeRange{
			From: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2024, 10, 1, 0, 5, 0, 0, time.UTC),
		},
		IntervalSize: 1 * time.Minute,
	}).Execute(context.Background(), unreachableClient)
	assert.Equal(t, backend.StatusInternal, got.Status, "DataResponse.Status")
	assert.Empty(t, got.Frames, "DataResponse.Frames")
	assert.Equal(t, backend.ErrorSourcePlugin, got.ErrorSource, "DataResponse.ErrorSource")
	assert.Error(t, got.Error, "DataResponse.Error")
}
