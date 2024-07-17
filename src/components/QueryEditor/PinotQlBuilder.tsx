import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React, { useState } from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { canRunQuery, PinotDataQuery } from '../../types/PinotDataQuery';
import { fetchSqlPreview } from '../../resources/sqlPreview';
import { useTableSchema } from '../../resources/controller';
import { NumericPinotDataTypes } from '../../types/PinotDataType';
import { SelectGranularity } from './SelectGranularity';

const MetricColumnStar = '*';
const AggregationFunctionCount = 'COUNT';
const AggregationFunctionNone = 'NONE';

export function PinotQlBuilder(props: PinotQueryEditorProps) {
  const { data, datasource, query, range, onChange, onRunQuery } = props;
  const [sqlPreview, setSqlPreview] = useState('');

  const tableSchema = useTableSchema(datasource, query.databaseName, query.tableName);

  const dateTimeFieldSpecs = tableSchema?.dateTimeFieldSpecs || [];
  const metricFieldSpecs = tableSchema?.metricFieldSpecs || [];
  const dimensionFieldSpecs = tableSchema?.dimensionFieldSpecs || [];

  const timeColumns = tableSchema ? dateTimeFieldSpecs.map((spec) => spec.name) : undefined;
  const metricColumns = tableSchema
    ? [...metricFieldSpecs, ...dimensionFieldSpecs]
        .filter((spec) => !query.groupByColumns?.includes(spec.name))
        // TODO: Is this filter necessary?
        .filter((spec) => NumericPinotDataTypes.includes(spec.dataType))
        .map((spec) => spec.name)
    : undefined;

  const dimensionColumns = tableSchema
    ? [...dimensionFieldSpecs, ...metricFieldSpecs]
        .filter((spec) => query.metricColumn !== spec.name)
        .map((spec) => spec.name)
    : undefined;

  const updateSqlPreview = (dataQuery: PinotDataQuery) => {
    fetchSqlPreview(datasource, {
      aggregationFunction: dataQuery.aggregationFunction,
      databaseName: dataQuery.databaseName,
      groupByColumns: dataQuery.groupByColumns,
      intervalSize: data?.request?.interval || '0',
      metricColumn: dataQuery.metricColumn,
      tableName: dataQuery.tableName,
      timeColumn: dataQuery.timeColumn,
      timeRange: { to: props.data?.request?.range.to, from: props.data?.request?.range.from },
      filters: dataQuery.filters,
      limit: dataQuery.limit,
      granularity: dataQuery.granularity,
    }).then((val) => val && setSqlPreview(val));
  };

  if (!sqlPreview) {
    updateSqlPreview(query);
  }

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    if (canRunQuery(newQuery)) {
      updateSqlPreview(newQuery);
      onRunQuery();
    }
  };

  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectTimeColumn
          selected={query.timeColumn}
          options={timeColumns}
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
          disabled={query.aggregationFunction === AggregationFunctionCount}
          onChange={(metricColumn) => onChangeAndRun({ ...query, metricColumn })}
        />
        <SelectAggregation
          selected={query.aggregationFunction}
          onChange={(aggregationFunction) => onChangeAndRun({ ...query, aggregationFunction })}
        />
      </div>
      <div>
        <SelectGroupBy
          selected={query.groupByColumns}
          options={dimensionColumns}
          disabled={query.aggregationFunction === AggregationFunctionNone}
          onChange={(values) => onChangeAndRun({ ...query, groupByColumns: values })}
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
        <SqlPreview sql={sqlPreview} />
      </div>
    </>
  );
}
