import { SelectMetricColumn } from './SelectMetricColumn';
import { AggregationFunction, SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React, { useEffect, useState } from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { interpolateVariables, PinotDataQuery } from '../../types/PinotDataQuery';
import { useTableSchema } from '../../resources/controller';
import { NumericPinotDataTypes } from '../../types/PinotDataType';
import { SelectPinotGranularity } from './SelectPinotGranularity';
import { SelectTable } from './SelectTable';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { DateTime, ScopedVars } from '@grafana/data';
import { DataSource } from '../../datasource';
import { TableSchema } from '../../types/TableSchema';
import { InputMetricLegend } from './InputMetricLegend';
import { previewSqlBuilder, PreviewSqlBuilderRequest } from '../../resources/previewSql';

const MetricColumnStar = '*';

export function PinotQlBuilder(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  tables: string[] | undefined;
  scopedVars: ScopedVars;
  onChange: (value: PinotDataQuery) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, tables, intervalSize, datasource, query, scopedVars, onChange, onRunQuery } = props;

  const tableSchema = useTableSchema(datasource, query.tableName);
  const sqlPreview = useSqlPreview(datasource, intervalSize, timeRange, query, scopedVars);

  function canRunQuery(query: PinotDataQuery) {
    return !!(
      query.tableName &&
      query.timeColumn &&
      query.aggregationFunction &&
      (query.metricColumn || query.aggregationFunction === AggregationFunction.COUNT)
    );
  }

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    const interpolated = interpolateVariables(newQuery, scopedVars);
    if (canRunQuery(interpolated)) {
      onRunQuery();
    }
  };

  const isSchemaLoading = query.tableName !== undefined && tableSchema === undefined;
  const timeColumns = getTimeColumns(tableSchema);
  const metricColumns = getMetricColumns(tableSchema, query.groupByColumns || []);
  const dimensionColumns = getGroupByColumns(tableSchema, query.metricColumn || '');

  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'} data-testid="select-table">
          <SelectTable
            options={tables}
            selected={query.tableName}
            onChange={(value: string | undefined) => onChange({ ...query, tableName: value, filters: undefined })}
          />
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectTimeColumn
          selected={query.timeColumn}
          timeColumns={timeColumns}
          isLoading={isSchemaLoading}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
        <SelectPinotGranularity
          selected={query.granularity}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          onChange={(value) => onChangeAndRun({ ...query, granularity: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={query.aggregationFunction === AggregationFunction.COUNT ? MetricColumnStar : query.metricColumn}
          metricColumns={query.aggregationFunction === AggregationFunction.COUNT ? [MetricColumnStar] : metricColumns}
          isLoading={isSchemaLoading}
          disabled={query.aggregationFunction === AggregationFunction.COUNT}
          onChange={(metricColumn) => onChangeAndRun({ ...query, metricColumn })}
        />
        <SelectAggregation
          selected={query.aggregationFunction}
          onChange={(aggregationFunction) => onChangeAndRun({ ...query, aggregationFunction })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectGroupBy
          selected={query.groupByColumns}
          options={dimensionColumns}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          isLoading={isSchemaLoading}
          onChange={(values) => onChangeAndRun({ ...query, groupByColumns: values })}
        />
        <SelectOrderBy
          selected={query.orderBy}
          columnNames={['time', 'metric', ...(query.groupByColumns || [])]}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          onChange={(orderBy) => onChangeAndRun({ ...query, orderBy })}
        />
      </div>

      <div>
        <SelectFilters
          datasource={datasource}
          tableSchema={tableSchema}
          tableName={query.tableName}
          timeColumn={query.timeColumn}
          timeRange={timeRange}
          dimensionColumns={dimensionColumns}
          dimensionFilters={query.filters || []}
          onChange={(val) => onChangeAndRun({ ...query, filters: val })}
        />
      </div>
      <div>
        <SelectQueryOptions
          selected={query.queryOptions || []}
          onChange={(queryOptions) => onChangeAndRun({ ...query, queryOptions })}
        />
      </div>
      <div>
        <InputLimit current={query.limit} onChange={(limit) => onChangeAndRun({ ...query, limit })} />
      </div>

      <div>
        <SqlPreview sql={sqlPreview} />
      </div>
      <div>
        <InputMetricLegend current={query.legend} onChange={(legend) => onChangeAndRun({ ...query, legend })} />
      </div>
    </>
  );
}

function getTimeColumns(tableSchema: TableSchema | undefined): string[] {
  return (tableSchema?.dateTimeFieldSpecs || []).map(({ name }) => name).sort();
}

function getMetricColumns(tableSchema: TableSchema | undefined, groupByColumns: string[]): string[] {
  return [...(tableSchema?.metricFieldSpecs || []), ...(tableSchema?.dimensionFieldSpecs || [])]
    .filter(({ name }) => name && !groupByColumns.includes(name))
    .filter(({ dataType }) => NumericPinotDataTypes.includes(dataType))
    .map(({ name }) => name)
    .sort();
}

function getGroupByColumns(tableSchema: TableSchema | undefined, metricColumn: string): string[] {
  return [...(tableSchema?.dimensionFieldSpecs || []), ...(tableSchema?.metricFieldSpecs || [])]
    .filter(({ name }) => name && metricColumn !== name)
    .map(({ name }) => name);
}

function useSqlPreview(
  datasource: DataSource,
  intervalSize: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  query: PinotDataQuery,
  scopedVars: ScopedVars
): string {
  const [sqlPreview, setSqlPreview] = useState('');

  const interpolated = interpolateVariables(query, scopedVars);
  const previewRequest: PreviewSqlBuilderRequest = {
    intervalSize: intervalSize,
    timeRange: timeRange,
    expandMacros: true,
    aggregationFunction: interpolated.aggregationFunction,
    groupByColumns: interpolated.groupByColumns,
    metricColumn: interpolated.metricColumn,
    tableName: interpolated.tableName,
    timeColumn: interpolated.timeColumn,
    filters: interpolated.filters,
    limit: interpolated.limit,
    granularity: interpolated.granularity,
    orderBy: interpolated.orderBy,
    queryOptions: interpolated.queryOptions,
  };

  useEffect(() => {
    previewSqlBuilder(datasource, previewRequest).then((val) => val && setSqlPreview(val));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return sqlPreview;
}
