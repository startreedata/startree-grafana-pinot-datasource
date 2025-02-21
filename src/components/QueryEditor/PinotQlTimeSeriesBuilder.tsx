import { SelectMetricColumn } from './SelectMetricColumn';
import { AggregationFunction, SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React from 'react';
import { InputLimit, InputSeriesLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectGranularity } from './SelectGranularity';
import { SelectTable } from './SelectTable';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { TimeSeriesBuilder } from '../../pinotql';
import { columnLabelOf } from '../../pinotql/complexField';

export function PinotQlTimeSeriesBuilder(props: {
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  savedParams: TimeSeriesBuilder.Params;
  interpolatedParams: TimeSeriesBuilder.Params;
  onChange: (newParams: TimeSeriesBuilder.Params) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, intervalSize, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;

  const resources = TimeSeriesBuilder.useResources(datasource, timeRange, intervalSize, interpolatedParams);
  const onChangeAndRun = (newParams: TimeSeriesBuilder.Params) => {
    onChange(newParams);
    if (TimeSeriesBuilder.canRunQuery(newParams)) {
      onRunQuery();
    }
  };

  if (TimeSeriesBuilder.applyDefaults(savedParams, resources)) {
    onChangeAndRun({ ...savedParams });
  }
  return (
    <>
      <SelectTable
        options={resources.tables}
        selected={savedParams.tableName}
        isLoading={resources.isTablesLoading}
        onChange={(tableName) => onChange({ ...savedParams, tableName })}
      />
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectTimeColumn
          selected={savedParams.timeColumn}
          timeColumns={resources.timeColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(timeColumn) => onChangeAndRun({ ...savedParams, timeColumn: timeColumn })}
        />
        <SelectGranularity
          selected={savedParams.granularity}
          disabled={savedParams.aggregationFunction === AggregationFunction.NONE}
          options={resources.granularities}
          isLoading={resources.isGranularitiesLoading}
          onChange={(granularity) => onChangeAndRun({ ...savedParams, granularity })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={savedParams.metricColumn}
          metricColumns={resources.metricColumns}
          isLoading={resources.isColumnsLoading}
          isCount={savedParams.aggregationFunction === AggregationFunction.COUNT}
          onChange={(metricColumn) => onChangeAndRun({ ...savedParams, metricColumn })}
        />
        <SelectAggregation
          selected={savedParams.aggregationFunction}
          onChange={(aggregationFunction) => onChangeAndRun({ ...savedParams, aggregationFunction })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectGroupBy
          selected={savedParams.groupByColumns}
          columns={resources.groupByColumns.filter(
            ({ name, key }) =>
              columnLabelOf(savedParams.metricColumn.name, savedParams.metricColumn.key) !== columnLabelOf(name, key)
          )}
          disabled={savedParams.aggregationFunction === AggregationFunction.NONE}
          isLoading={resources.isColumnsLoading}
          onChange={(groupByColumns) => onChangeAndRun({ ...savedParams, groupByColumns })}
        />
        <SelectOrderBy
          selected={savedParams.orderBy}
          columns={[{ name: '__time' }, { name: '__metric' }, ...savedParams.groupByColumns]}
          disabled={savedParams.aggregationFunction === AggregationFunction.NONE}
          onChange={(orderBy) => onChangeAndRun({ ...savedParams, orderBy })}
        />
      </div>

      <SelectFilters
        datasource={datasource}
        tableName={savedParams.tableName}
        timeColumn={savedParams.timeColumn}
        timeRange={timeRange}
        columns={resources.filterColumns}
        filters={savedParams.filters}
        isColumnsLoading={resources.isColumnsLoading}
        onChange={(filters) => onChangeAndRun({ ...savedParams, filters })}
      />
      <SelectQueryOptions
        selected={savedParams.queryOptions}
        onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
      />
      <InputLimit current={savedParams.limit} onChange={(limit) => onChangeAndRun({ ...savedParams, limit })} />
      <SqlPreview sql={resources.sqlPreview} />
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputMetricLegend
          current={savedParams.legend}
          onChange={(legend) => onChangeAndRun({ ...savedParams, legend })}
        />
        <InputSeriesLimit
          current={savedParams.seriesLimit}
          onChange={(seriesLimit) => onChangeAndRun({ ...savedParams, seriesLimit })}
        />
      </div>
    </>
  );
}
