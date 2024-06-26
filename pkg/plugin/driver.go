package plugin

import (
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/startreedata/pinot-client-go/pinot"
)

type Driver interface {
	RenderPinotSql() (string, error)
	ExtractResults(results *pinot.ResultTable) (*data.Frame, error)
}

func BuildDriver(query PinotDataQuery, tableSchema TableSchema, timeRange backend.TimeRange) (Driver, error) {
	if query.QueryType == "PinotQL" {
		if query.EditorMode == "Builder" {
			Logger.Info("constructed pinot-ql-builder driver")
			return NewPinotQlBuilderDriver(PinotQlBuilderParams{
				TableSchema:         tableSchema,
				TimeRange:           TimeRange{To: timeRange.To, From: timeRange.From},
				IntervalSize:        query.IntervalSize,
				DatabaseName:        query.DatabaseName,
				TableName:           query.TableName,
				TimeColumn:          query.TimeColumn,
				MetricColumn:        query.MetricColumn,
				DimensionColumns:    query.DimensionColumns,
				AggregationFunction: query.AggregationFunction,
			})
		} else {
			Logger.Info("constructed pinot-ql-code driver")
			return NewPinotQlCodeDriver(query, tableSchema, TimeRange{To: timeRange.To, From: timeRange.From}), nil
		}
	}
	Logger.Info("constructed no-op driver")
	return &NoOpDriver{}, nil
}

var _ Driver = &NoOpDriver{}

type NoOpDriver struct{}

func (d *NoOpDriver) RenderPinotSql() (string, error) {
	Logger.Info("no-op rendering sql")
	return "select 1", nil
}
func (d *NoOpDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	Logger.Info("no-op extracting results")
	return data.NewFrame("results"), nil
}
