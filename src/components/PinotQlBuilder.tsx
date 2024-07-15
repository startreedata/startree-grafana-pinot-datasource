import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { canRunQuery, PinotDataQuery } from '../types/PinotDataQuery';
import { useSqlPreview } from '../resources/sqlPreview';
import { useTableSchema } from '../resources/controller';
import { NumericPinotDataTypes } from '../types/PinotDataType';
import { SelectGranularity } from './SelectGranularity';

const AggregationFunctionNone = 'NONE';

export function PinotQlBuilder(props: PinotQueryEditorProps) {
  const { data, datasource, query, range, onChange, onRunQuery } = props;

  const sql = useSqlPreview(datasource, {
    aggregationFunction: query.aggregationFunction,
    databaseName: query.databaseName,
    groupByColumns: query.groupByColumns,
    intervalSize: data?.request?.interval || '0',
    metricColumn: query.metricColumn,
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: { to: range?.to, from: range?.from },
    filters: query.filters,
    limit: query.limit,
  });
  const tableSchema = useTableSchema(datasource, query.databaseName, query.tableName);

  const timeColumns = tableSchema?.dateTimeFieldSpecs.map((spec) => spec.name);
  const metricColumns = tableSchema
    ? [...tableSchema.metricFieldSpecs, ...tableSchema.dimensionFieldSpecs]
        .filter((spec) => !query.groupByColumns?.includes(spec.name))
        // TODO: Is this filter necessary?
        .filter((spec) => NumericPinotDataTypes.includes(spec.dataType))
        .map((spec) => spec.name)
    : undefined;

  const dimensionColumns = tableSchema
    ? [...tableSchema.dimensionFieldSpecs, ...tableSchema.metricFieldSpecs]
        .filter((spec) => query.metricColumn !== spec.name)
        .map((spec) => spec.name)
    : undefined;

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    if (canRunQuery(newQuery)) {
      onRunQuery();
    }
  };

  return (
    <>
      <div>
        <SelectTimeColumn
          selected={query.timeColumn}
          options={timeColumns}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={query.metricColumn}
          options={metricColumns}
          onChange={(value) => onChangeAndRun({ ...query, metricColumn: value })}
        />
        <SelectAggregation
          selected={query.aggregationFunction}
          onChange={(value) => onChangeAndRun({ ...query, aggregationFunction: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectGroupBy
          selected={query.groupByColumns}
          options={dimensionColumns}
          disabled={query.aggregationFunction === AggregationFunctionNone}
          onChange={(values) => onChangeAndRun({ ...query, groupByColumns: values })}
        />
        <SelectGranularity
          selected={query.granularity}
          disabled={query.aggregationFunction === AggregationFunctionNone}
          onChange={(value) => onChangeAndRun({ ...query, granularity: value })}
        />
      </div>
      <div>
        <SelectFilters
          datasource={datasource}
          databaseName={query.databaseName}
          tableSchema={tableSchema}
          tableName={query.tableName}
          timeColumn={query.timeColumn}
          range={range}
          dimensionColumns={dimensionColumns}
          dimensionFilters={query.filters || []}
          onChange={(val) => onChangeAndRun({ ...props.query, filters: val })}
        />
      </div>
      <div>
        <InputLimit current={query.limit} onChange={(val) => onChangeAndRun({ ...props.query, limit: val })} />
      </div>
      <div>
        <SqlPreview sql={sql} />
      </div>
    </>
  );
}
