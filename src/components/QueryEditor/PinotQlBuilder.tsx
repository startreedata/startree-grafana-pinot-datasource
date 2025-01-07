import { SelectMetricColumn } from './SelectMetricColumn';
import { AggregationFunction, SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React, { useEffect } from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectGranularity } from './SelectGranularity';
import { SelectTable } from './SelectTable';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { columnLabelOf } from '../../dataquery/ComplexField';
import { applyBuilderDefaults, BuilderParams, canRunBuilderQuery } from '../../pinotql/builderParams';
import { useBuilderResources } from '../../pinotql/builderResources';

export function PinotQlBuilder(props: {
  datasource: DataSource;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  savedParams: BuilderParams;
  interpolatedParams: BuilderParams;
  onChange: (newParams: BuilderParams) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, intervalSize, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;

  const resources = useBuilderResources(datasource, timeRange, intervalSize, interpolatedParams);

  const onChangeAndRun = (newParams: BuilderParams) => {
    onChange(newParams);
    if (canRunBuilderQuery(newParams)) {
      onRunQuery();
    }
  };

  useEffect(() => {
    if (applyBuilderDefaults(savedParams, resources)) {
      onChangeAndRun({ ...savedParams });
    }
  });

  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <SelectTable
          options={resources.tables}
          selected={savedParams.tableName}
          isLoading={resources.isTablesLoading}
          onChange={(tableName) => onChange({ ...savedParams, tableName })}
        />
      </div>
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
          columns={[{ name: 'time' }, { name: 'metric' }, ...savedParams.groupByColumns]}
          disabled={savedParams.aggregationFunction === AggregationFunction.NONE}
          onChange={(orderBy) => onChangeAndRun({ ...savedParams, orderBy })}
        />
      </div>

      <div>
        <SelectFilters
          datasource={datasource}
          tableName={savedParams.tableName}
          timeColumn={savedParams.timeColumn}
          timeRange={timeRange}
          dimensionColumns={resources.filterColumns}
          dimensionFilters={savedParams.filters}
          onChange={(filters) => onChangeAndRun({ ...savedParams, filters })}
        />
      </div>
      <div>
        <SelectQueryOptions
          selected={savedParams.queryOptions}
          onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
        />
      </div>
      <div>
        <InputLimit current={savedParams.limit} onChange={(limit) => onChangeAndRun({ ...savedParams, limit })} />
      </div>

      <div>
        <SqlPreview sql={resources.sqlPreview} />
      </div>
      <div>
        <InputMetricLegend
          current={savedParams.legend}
          onChange={(legend) => onChangeAndRun({ ...savedParams, legend })}
        />
      </div>
    </>
  );
}
