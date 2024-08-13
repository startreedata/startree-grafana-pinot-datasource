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

func NewDriver(query PinotDataQuery, tableSchema TableSchema, timeRange backend.TimeRange) (Driver, error) {
	if query.QueryType == "PinotQL" {
		switch true {
		case query.TableName == "":
			// Don't return an error when a user first lands on the query editor.
			return &NoOpDriver{}, nil
		case query.EditorMode == "Builder":
			return NewPinotQlBuilderDriver(PinotQlBuilderParams{
				TableSchema:         tableSchema,
				TimeRange:           TimeRange{To: timeRange.To, From: timeRange.From},
				IntervalSize:        query.IntervalSize,
				DatabaseName:        query.DatabaseName,
				TableName:           query.TableName,
				TimeColumn:          query.TimeColumn,
				MetricColumn:        query.MetricColumn,
				GroupByColumns:      query.GroupByColumns,
				AggregationFunction: query.AggregationFunction,
				DimensionFilters:    query.DimensionFilters,
				Limit:               query.Limit,
				Granularity:         query.Granularity,
				OrderByClauses:      query.OrderByClauses,
				QueryOptions:        query.QueryOptions,
			})
		default:
			if query.PinotQlCode == "" {
				return &NoOpDriver{}, nil
			}
			return NewPinotQlCodeDriver(PinotQlCodeDriverParams{
				Code:              query.PinotQlCode,
				DatabaseName:      query.DatabaseName,
				TableName:         query.TableName,
				TimeColumnAlias:   query.TimeColumnAlias,
				TimeColumnFormat:  query.TimeColumnFormat,
				MetricColumnAlias: query.MetricColumnAlias,
				TimeRange:         TimeRange{To: timeRange.To, From: timeRange.From},
				IntervalSize:      query.IntervalSize,
				TableSchema:       tableSchema,
				DisplayType:       query.DisplayType,
			})
		}
	}
	return &NoOpDriver{}, nil
}

var _ Driver = &NoOpDriver{}

type NoOpDriver struct{}

func (d *NoOpDriver) RenderPinotSql() (string, error) {
	return "select 1", nil
}
func (d *NoOpDriver) ExtractResults(results *pinot.ResultTable) (*data.Frame, error) {
	return data.NewFrame("results"), nil
}
