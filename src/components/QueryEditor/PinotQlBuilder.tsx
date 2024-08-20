import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React, { useCallback, useEffect, useState } from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { fetchSqlPreview } from '../../resources/sqlPreview';
import { useTableSchema } from '../../resources/controller';
import { NumericPinotDataTypes } from '../../types/PinotDataType';
import { SelectGranularity } from './SelectGranularity';
import { SelectTable } from './SelectTable';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import {TableSchema} from "../../types/TableSchema";

const MetricColumnStar = '*';
const AggregationFunctionCount = 'COUNT';
const AggregationFunctionNone = 'NONE';

export function PinotQlBuilder(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  tables: string[] | undefined;
  onChange: (value: PinotDataQuery) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, tables, intervalSize, datasource, query, onChange, onRunQuery } = props;

  const tableSchema = useTableSchema(datasource, query.tableName);

  const [sqlPreview, setSqlPreview] = useState('');

  const updateSqlPreview = useCallback(
    (dataQuery: PinotDataQuery) => {
      fetchSqlPreview(datasource, {
        aggregationFunction: dataQuery.aggregationFunction,
        groupByColumns: dataQuery.groupByColumns,
        intervalSize: intervalSize || '0',
        metricColumn: dataQuery.metricColumn,
        tableName: dataQuery.tableName,
        timeColumn: dataQuery.timeColumn,
        timeRange: timeRange,
        filters: dataQuery.filters,
        limit: dataQuery.limit,
        granularity: dataQuery.granularity,
        orderBy: dataQuery.orderBy,
        queryOptions: dataQuery.queryOptions,
      }).then((val) => val && setSqlPreview(val));
    },
    [datasource, intervalSize, timeRange]
  );

  useEffect(() => {
    if (!sqlPreview) {
      updateSqlPreview(query);
    }
  }, [sqlPreview, query, updateSqlPreview]);

  const canRunQuery = (newQuery: PinotDataQuery) => {
    return !!(
      newQuery.tableName &&
      newQuery.timeColumn &&
      newQuery.aggregationFunction &&
      (newQuery.metricColumn || newQuery.aggregationFunction === 'COUNT')
    );
  };

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    if (canRunQuery(newQuery)) {
      updateSqlPreview(newQuery);
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
        <div className={'gf-form'}>
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
          options={timeColumns}
          isLoading={isSchemaLoading}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
        <SelectGranularity
          selected={query.granularity}
          disabled={query.aggregationFunction === AggregationFunctionNone}
          onChange={(value) => onChangeAndRun({ ...query, granularity: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={query.aggregationFunction === AggregationFunctionCount ? MetricColumnStar : query.metricColumn}
          options={query.aggregationFunction === AggregationFunctionCount ? [MetricColumnStar] : metricColumns}
          isLoading={isSchemaLoading}
          disabled={query.aggregationFunction === AggregationFunctionCount}
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
          disabled={query.aggregationFunction === AggregationFunctionNone}
          isLoading={isSchemaLoading}
          onChange={(values) => onChangeAndRun({ ...query, groupByColumns: values })}
        />
        <SelectOrderBy
          selected={query.orderBy}
          columnNames={['time', 'metric', ...(query.groupByColumns || [])]}
          disabled={query.aggregationFunction === AggregationFunctionNone}
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
        <InputLimit current={query.limit} onChange={(limit) => onChangeAndRun({ ...query, limit: limit })} />
      </div>
      <div>
        <SqlPreview sql={sqlPreview} />
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
