package dataquery

import (
	"context"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/pinotlib"
	"github.com/startreedata/startree-grafana-pinot-datasource/pkg/plugin/templates"
)

type LogsBuilderParams struct {
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

func (params *LogsBuilderParams) Validate() error {
	switch {
	case params.TableName == "":
		return fmt.Errorf("table name is required")
	case params.LogColumn.Name == "":
		return fmt.Errorf("log column name is required")
	case params.TimeColumn == "":
		return fmt.Errorf("time column is required")
	default:
		return nil
	}
}

func (params *LogsBuilderParams) ApplyDefaults() {
	if params.Limit <= 0 {
		params.Limit = DefaultLimit
	}
	if params.LogColumnAlias == "" {
		params.LogColumnAlias = complexFieldAlias(params.LogColumn.Name, params.LogColumn.Key)
	}
}

func ExecuteLogsBuilderQuery(ctx context.Context, client *pinotlib.PinotClient, params LogsBuilderParams) backend.DataResponse {
	params.ApplyDefaults()
	if err := params.Validate(); err != nil {
		return NewPluginErrorResponse(err)
	}

	tableSchema, err := client.GetTableSchema(ctx, params.TableName)
	if err != nil {
		return NewDownstreamErrorResponse(err)
	}

	sql, err := RenderLogsBuilderSql(tableSchema, params)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	results, exceptions, ok, backendResp := doSqlQuery(ctx, client, pinotlib.NewSqlQuery(sql))
	if !ok {
		return backendResp
	}

	var logColumn string
	if params.LogColumnAlias == "" {
		logColumn = params.LogColumn.Name
	} else {
		logColumn = params.LogColumnAlias
	}

	frame, err := ExtractLogsDataFrame(results, params.TimeColumn, logColumn)
	if err != nil {
		return NewPluginErrorResponse(err)
	}

	return NewSqlQueryDataResponse(frame, exceptions)
}

func RenderLogsBuilderSql(tableSchema pinotlib.TableSchema, params LogsBuilderParams) (string, error) {
	timeColumnFormat, err := pinotlib.GetTimeColumnFormat(tableSchema, params.TimeColumn)
	if err != nil {
		return "", err
	}

	timeFilterExpr := pinotlib.TimeFilterExpr(pinotlib.TimeFilter{
		Column: params.TimeColumn,
		Format: timeColumnFormat,
		From:   params.TimeRange.From,
		To:     params.TimeRange.To,
	})

	return templates.RenderLogSql(templates.LogSqlParams{
		TableNameExpr:        pinotlib.ObjectExpr(params.TableName),
		TimeColumn:           params.TimeColumn,
		LogColumnExpr:        pinotlib.ComplexFieldExpr(params.LogColumn.Name, params.LogColumn.Key),
		LogColumnAlias:       params.LogColumnAlias,
		MetadataColumns:      logsMetadataColumnsFrom(params),
		TimeFilterExpr:       timeFilterExpr,
		DimensionFilterExprs: FilterExprsFrom(params.DimensionFilters),
		Limit:                params.Limit,
		QueryOptionsExpr:     QueryOptionsExpr(params.QueryOptions),
	})
}

func RenderLogsBuilderSqlWithMacros(params LogsBuilderParams) (string, error) {
	return templates.RenderLogSql(templates.LogSqlParams{
		TableNameExpr:        MacroExprFor(MacroTable),
		TimeColumn:           params.TimeColumn,
		LogColumnExpr:        pinotlib.ComplexFieldExpr(params.LogColumn.Name, params.LogColumn.Key),
		LogColumnAlias:       params.LogColumnAlias,
		MetadataColumns:      logsMetadataColumnsFrom(params),
		TimeFilterExpr:       MacroExprFor(MacroTimeFilter, pinotlib.ObjectExpr(params.TimeColumn)),
		DimensionFilterExprs: FilterExprsFrom(params.DimensionFilters),
		Limit:                params.Limit,
		QueryOptionsExpr:     QueryOptionsExpr(params.QueryOptions),
	})
}

func logsMetadataColumnsFrom(params LogsBuilderParams) []templates.ExprWithAlias {
	var metadataColumns []templates.ExprWithAlias

	for _, column := range params.MetadataColumns {
		metadataColumns = append(metadataColumns, templates.ExprWithAlias{
			Expr:  pinotlib.ComplexFieldExpr(column.Name, column.Key),
			Alias: complexFieldAlias(column.Name, column.Key),
		})
	}

	for _, extractor := range params.JsonExtractors {
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

	for _, extractor := range params.RegexpExtractors {
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
