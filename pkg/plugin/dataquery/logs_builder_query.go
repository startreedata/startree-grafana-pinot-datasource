package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/pinot"
)

var _ ExecutableQuery = LogsBuilderQuery{}

type LogsBuilderQuery struct {
	TimeRange        TimeRange
	TableName        string
	TimeColumn       string
	LogColumn        ComplexField
	LogColumnAlias   string
	MetadataColumns  []ComplexField
	JsonExtractors   []JsonExtractor
	RegexpExtractors []RegexpExtractor
	DimensionFilters []DimensionFilter
	QueryOptions     []QueryOption
	Limit            int64
}

func (query LogsBuilderQuery) Validate() error {
	switch {
	case query.TableName == "":
		return fmt.Errorf("table name is required")
	case query.LogColumn.Name == "":
		return fmt.Errorf("log column name is required")
	case query.TimeColumn == "":
		return fmt.Errorf("time column is required")
	default:
		return nil
	}
}

func (query LogsBuilderQuery) Execute(client *pinot.Client, ctx context.Context) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewBadRequestErrorResponse(err)
	}

	sqlQuery, err := query.RenderSqlQuery(ctx, client)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, sqlQuery)
	if !ok {
		return backendResp
	}

	frame, err := ExtractLogsDataFrame(results, query.TimeColumn, BuilderLogColumn)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query LogsBuilderQuery) RenderSqlQuery(ctx context.Context, client *pinot.Client) (pinot.SqlQuery, error) {
	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	timeColumnFormat, err := pinot.GetTimeColumnFormat(tableSchema, query.TimeColumn)
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	sql, err := pinot.RenderLogSql(pinot.LogSqlParams{
		TableNameExpr:        pinot.ObjectExpr(query.TableName),
		TimeColumn:           query.TimeColumn,
		LogColumnExpr:        pinot.ComplexFieldExpr(query.LogColumn.Name, query.LogColumn.Key),
		LogColumnAlias:       BuilderLogColumn,
		MetadataColumns:      query.logsMetadataColumns(),
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		Limit:                query.resolveLimit(),
		TimeFilterExpr: pinot.TimeFilterExpr(pinot.TimeFilter{
			Column: query.TimeColumn,
			Format: timeColumnFormat,
			From:   query.TimeRange.From,
			To:     query.TimeRange.To,
		}),
	})
	if err != nil {
		return pinot.SqlQuery{}, err
	}

	return newSqlQueryWithOptions(sql, query.QueryOptions), nil
}

func (query LogsBuilderQuery) RenderSqlWithMacros() (string, error) {
	sql, err := pinot.RenderLogSql(pinot.LogSqlParams{
		TableNameExpr:        MacroExprFor(MacroTable),
		TimeColumn:           query.TimeColumn,
		LogColumnExpr:        pinot.ComplexFieldExpr(query.LogColumn.Name, query.LogColumn.Key),
		LogColumnAlias:       BuilderLogColumn,
		MetadataColumns:      query.logsMetadataColumns(),
		TimeFilterExpr:       MacroExprFor(MacroTimeFilter, pinot.ObjectExpr(query.TimeColumn).String()),
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		Limit:                query.resolveLimit(),
	})
	if err != nil {
		return "", err
	}
	return newSqlQueryWithOptions(sql, query.QueryOptions).RenderSql(), nil

}

func (query LogsBuilderQuery) logsMetadataColumns() []pinot.ExprWithAlias {
	var metadataColumns []pinot.ExprWithAlias

	for _, column := range query.MetadataColumns {
		metadataColumns = append(metadataColumns, pinot.ExprWithAlias{
			Expr:  pinot.ComplexFieldExpr(column.Name, column.Key),
			Alias: complexFieldAlias(column.Name, column.Key),
		})
	}

	for _, extractor := range query.JsonExtractors {
		if extractor.Source.Name == "" || extractor.Path == "" || extractor.ResultType == "" {
			continue
		}

		var defaultValueExpr pinot.SqlExpr
		switch extractor.ResultType {
		case pinot.DataTypeInt, pinot.DataTypeLong, pinot.DataTypeFloat, pinot.DataTypeDouble:
			defaultValueExpr = pinot.LiteralExpr(0)
		case pinot.DataTypeBoolean:
			defaultValueExpr = pinot.LiteralExpr(false)
		default:
			defaultValueExpr = pinot.LiteralExpr("")
		}
		columnExpr := pinot.ComplexFieldExpr(extractor.Source.Name, extractor.Source.Key)
		metadataColumns = append(metadataColumns, pinot.ExprWithAlias{
			Expr:  pinot.JsonExtractScalarExpr(columnExpr, extractor.Path, extractor.ResultType, defaultValueExpr),
			Alias: extractor.Alias,
		})
	}

	for _, extractor := range query.RegexpExtractors {
		if extractor.Source.Name == "" || extractor.Pattern == "" {
			continue
		}

		columnExpr := pinot.ComplexFieldExpr(extractor.Source.Name, extractor.Source.Key)
		metadataColumns = append(metadataColumns, pinot.ExprWithAlias{
			Expr:  pinot.RegexpExtractExpr(columnExpr, extractor.Pattern, extractor.Group, pinot.LiteralExpr("")),
			Alias: extractor.Alias,
		})
	}
	return metadataColumns[:]
}

func (query LogsBuilderQuery) resolveLimit() int64 {
	if query.Limit <= 0 {
		return DefaultQueryLimit
	} else {
		return query.Limit
	}
}
