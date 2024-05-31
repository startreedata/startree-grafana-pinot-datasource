package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"time"
)

type QueryContext struct {
	QueryType         string
	TableName         string
	TableSchema       TableSchema
	IntervalSize      time.Duration
	TimeRange         backend.TimeRange
	SqlContext        SqlContext
	TimeSeriesContext TimeSeriesContext
}

func BuildQueryContext(client *PinotClient, ctx context.Context, query backend.DataQuery) (QueryContext, error) {
	var queryModel struct {
		QueryType    string  `json:"editorType"`
		TableName    string  `json:"tableName"`
		Fill         bool    `json:"fill"`
		FillInterval float64 `json:"fillInterval"`
		FillMode     string  `json:"fillMode"`
		FillValue    float64 `json:"fillValue"`
		Format       string  `json:"format"`
		RawSql       string  `json:"rawSql"`

		SqlContext        `json:"sqlContext"`
		TimeSeriesContext `json:"timeSeriesContext"`
	}

	err := json.Unmarshal(query.JSON, &queryModel)
	if err != nil {
		return QueryContext{}, fmt.Errorf("failed to unmarshal query model: %w", err)
	}
	Logger.Info("extracted query model", queryModel)

	tableSchema, err := client.GetTableSchema(ctx, queryModel.TableName)
	if err != nil {
		Logger.Error("failed to fetch table schema", "error", err)
		return QueryContext{}, fmt.Errorf("failed to fetch table schema: %w", err)
	}
	Logger.Info("extracted table schema", tableSchema)

	return QueryContext{
		TableName:         queryModel.TableName,
		TableSchema:       tableSchema,
		IntervalSize:      query.Interval,
		TimeRange:         query.TimeRange,
		SqlContext:        SqlContext{RawSql: queryModel.RawSql},
		TimeSeriesContext: queryModel.TimeSeriesContext,
	}, nil
}
