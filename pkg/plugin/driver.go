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
	switch query.QueryType {
	case "PinotSql":
		return NewSqlTableDriver(query, tableSchema, timeRange), nil
	case "TimeSeriesSql":
		return NewTimeSeriesDriver(TimeSeriesDriverParams{
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
	default:
		return &NoOpDriver{}, nil
	}
}

var _ Driver = &NoOpDriver{}

type NoOpDriver struct{}

func (d *NoOpDriver) RenderPinotSql() (string, error) {
	return "select 1", nil
}
func (d *NoOpDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	return data.NewFrame("results"), nil
}
