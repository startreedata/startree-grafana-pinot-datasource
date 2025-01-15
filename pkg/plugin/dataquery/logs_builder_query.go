package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
)

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

func (query LogsBuilderQuery) Execute(ctx context.Context, client *pinotlib.PinotClient) backend.DataResponse {
	if err := query.Validate(); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableSchema, err := client.GetTableSchema(ctx, query.TableName)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	sql, err := query.RenderSql(tableSchema)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	frame, err := ExtractLogsDataFrame(results, query.TimeColumn, BuilderLogColumn)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func (query LogsBuilderQuery) RenderSql(tableSchema pinotlib.TableSchema) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(tableSchema, query.TimeColumn)
	if err != nil {
		return "", err
	}

	timeFilterExpr := pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
		Column: query.TimeColumn,
		Format: timeColumnFormat,
		From:   query.TimeRange.From,
		To:     query.TimeRange.To,
	})

	return templates.RenderLogSql(templates.LogSqlParams{
		TableNameExpr:        pinotlib.ObjectExpr(query.TableName),
		TimeColumn:           query.TimeColumn,
		LogColumnExpr:        pinotlib.ComplexFieldExpr(query.LogColumn.Name, query.LogColumn.Key),
		LogColumnAlias:       BuilderLogColumn,
		MetadataColumns:      query.logsMetadataColumns(),
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		Limit:                query.resolveLimit(),
		QueryOptionsExpr:     QueryOptionsExpr(query.QueryOptions),
	})
}

func (query LogsBuilderQuery) RenderSqlWithMacros() (string, error) {
	return templates.RenderLogSql(templates.LogSqlParams{
		TableNameExpr:        MacroExprFor(MacroTable),
		TimeColumn:           query.TimeColumn,
		LogColumnExpr:        pinotlib.ComplexFieldExpr(query.LogColumn.Name, query.LogColumn.Key),
		LogColumnAlias:       BuilderLogColumn,
		MetadataColumns:      query.logsMetadataColumns(),
		TimeFilterExpr:       MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(query.TimeColumn)),
		DimensionFilterExprs: FilterExprsFrom(query.DimensionFilters),
		Limit:                query.resolveLimit(),
		QueryOptionsExpr:     QueryOptionsExpr(query.QueryOptions),
	})
}

func (query LogsBuilderQuery) logsMetadataColumns() []templates.ExprWithAlias {
	var metadataColumns []templates.ExprWithAlias

	for _, column := range query.MetadataColumns {
		metadataColumns = append(metadataColumns, templates.ExprWithAlias{
			Expr:  pinotlib.ComplexFieldExpr(column.Name, column.Key),
			Alias: complexFieldAlias(column.Name, column.Key),
		})
	}

	for _, extractor := range query.JsonExtractors {
		if extractor.Source.Name == "" || extractor.Path == "" || extractor.ResultType == "" {
			continue
		}

		var defaultValueExpr string
		switch extractor.ResultType {
		case pinotlib.DataTypeInt, pinotlib.DataTypeLong, pinotlib.DataTypeFloat, pinotlib.DataTypeDouble:
			defaultValueExpr = pinotlib.LiteralExpr(0)
		case pinotlib.DataTypeBoolean:
			defaultValueExpr = pinotlib.LiteralExpr(false)
		default:
			defaultValueExpr = pinotlib.LiteralExpr("")
		}
		columnExpr := pinotlib.ComplexFieldExpr(extractor.Source.Name, extractor.Source.Key)
		metadataColumns = append(metadataColumns, templates.ExprWithAlias{
			Expr:  pinotlib.JsonExtractScalarExpr(columnExpr, extractor.Path, extractor.ResultType, defaultValueExpr),
			Alias: extractor.Alias,
		})
	}

	for _, extractor := range query.RegexpExtractors {
		if extractor.Source.Name == "" || extractor.Pattern == "" {
			continue
		}

		columnExpr := pinotlib.ComplexFieldExpr(extractor.Source.Name, extractor.Source.Key)
		metadataColumns = append(metadataColumns, templates.ExprWithAlias{
			Expr:  pinotlib.RegexpExtractExpr(columnExpr, extractor.Pattern, extractor.Group, pinotlib.LiteralExpr("")),
			Alias: extractor.Alias,
		})
	}
	return metadataColumns[:]
}

func (query LogsBuilderQuery) resolveLimit() int64 {
	if query.Limit <= 0 {
		return DefaultLimit
	} else {
		return query.Limit
	}
}
