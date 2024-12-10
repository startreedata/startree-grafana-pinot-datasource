import { SelectMetricColumn } from './SelectMetricColumn';
import { AggregationFunction, SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React, { useEffect, useState } from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { SelectTimeColumn } from './SelectTimeColumn';
import {
  builderGroupByColumnsFrom,
  builderMetricColumnFrom,
  interpolateVariables,
  PinotDataQuery,
} from '../../types/PinotDataQuery';
import { SelectGranularity } from './SelectGranularity';
import { SelectTable } from './SelectTable';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { DateTime, ScopedVars } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { previewSqlBuilder, PreviewSqlBuilderRequest } from '../../resources/previewSql';
import { useGranularities } from '../../resources/granularities';
import { useColumns } from '../../resources/columns';
import { columnLabelOf, ComplexField } from '../../types/ComplexField';

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

  const sqlPreview = useSqlPreview(datasource, intervalSize, timeRange, query, scopedVars);

  const { result: granularities, loading: isGranularitiesLoading } = useGranularities(
    datasource,
    query.tableName,
    query.timeColumn
  );

  const { result: columns, loading: isColumnsLoading } = useColumns(datasource, {
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: timeRange,
    filters: query.filters || [],
  });

  function canRunQuery(query: PinotDataQuery) {
    return !!(
      query.tableName &&
      query.timeColumn &&
      query.aggregationFunction &&
      (builderMetricColumnFrom(query)?.name || query.aggregationFunction === AggregationFunction.COUNT)
    );
  }

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    const interpolated = interpolateVariables(newQuery, scopedVars);
    if (canRunQuery(interpolated)) {
      onRunQuery();
    }
  };

  const metricColumn = builderMetricColumnFrom(query);
  const selectedGroupBys = builderGroupByColumnsFrom(query);
  const allowedOrderBys: ComplexField[] = [{ name: 'time' }, { name: 'metric' }, ...selectedGroupBys];

  const timeColumns = columns.filter(({ isTime, isDerived }) => isTime && !isDerived);
  const dimensionColumns = columns.filter(({ isTime }) => !isTime);
  const metricColumns = columns.filter(({ isTime, isMetric }) => !isTime && isMetric);

  return (
    <>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'} data-testid="select-table">
          <SelectTable
            options={tables}
            selected={query.tableName}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                tableName: value,
                filters: undefined,
              })
            }
          />
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectTimeColumn
          selected={query.timeColumn}
          timeColumns={timeColumns}
          isLoading={isColumnsLoading}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
        <SelectGranularity
          selected={query.granularity}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          options={granularities}
          isLoading={isGranularitiesLoading}
          onChange={(value) => onChangeAndRun({ ...query, granularity: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={metricColumn}
          metricColumns={metricColumns}
          isLoading={isColumnsLoading}
          isCount={query.aggregationFunction === AggregationFunction.COUNT}
          onChange={(metricColumnV2) => onChangeAndRun({ ...query, metricColumnV2, metricColumn: undefined })}
        />
        <SelectAggregation
          selected={query.aggregationFunction}
          onChange={(aggregationFunction) => onChangeAndRun({ ...query, aggregationFunction })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectGroupBy
          selected={selectedGroupBys}
          columns={dimensionColumns.filter(
            ({ name, key }) => columnLabelOf(metricColumn?.name, metricColumn?.key) != columnLabelOf(name, key)
          )}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          isLoading={isColumnsLoading}
          onChange={(groupByColumnsV2) =>
            onChangeAndRun({
              ...query,
              groupByColumnsV2,
              groupByColumns: undefined,
            })
          }
        />
        <SelectOrderBy
          selected={query.orderBy}
          columns={allowedOrderBys}
          disabled={query.aggregationFunction === AggregationFunction.NONE}
          onChange={(orderBy) => onChangeAndRun({ ...query, orderBy })}
        />
      </div>

      <div>
        <SelectFilters
          datasource={datasource}
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
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    aggregationFunction: interpolated.aggregationFunction,
    groupByColumns: builderGroupByColumnsFrom(interpolated),
    metricColumn: builderMetricColumnFrom(interpolated),
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
  }, [datasource, query.queryType, query.editorMode, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return sqlPreview;
}
