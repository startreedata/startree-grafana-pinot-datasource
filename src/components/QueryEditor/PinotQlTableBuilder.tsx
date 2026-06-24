import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { SelectTable } from './SelectTable';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectGroupBy } from './SelectGroupBy';
import { SelectAggregations } from './SelectAggregations';
import { SelectFilters } from './SelectFilters';
import { SelectOrderBy } from './SelectOrderBy';
import { SelectQueryOptions } from './SelectQueryOptions';
import { InputLimit } from './InputLimit';
import { SqlPreview } from './SqlPreview';
import React, { useEffect } from 'react';
import { ComplexField } from '../../dataquery/ComplexField';
import { TableBuilder } from '../../pinotql';
import { useAutoSurfaceMultiStageEngine } from './useAutoSurfaceMultiStageEngine';

export function PinotQlTableBuilder(props: {
  savedParams: TableBuilder.Params;
  interpolatedParams: TableBuilder.Params;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  datasource: DataSource;
  onChange: (value: TableBuilder.Params) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;
  const resources = TableBuilder.useResources(datasource, timeRange, interpolatedParams);

  const onChangeAndRun = (newParams: TableBuilder.Params) => {
    onChange(newParams);
    if (TableBuilder.canRunQuery(newParams)) {
      onRunQuery();
    }
  };

  useEffect(() => {
    if (TableBuilder.applyDefaults(savedParams, resources)) {
      onChangeAndRun({ ...savedParams });
    }
  });

  useAutoSurfaceMultiStageEngine(savedParams, interpolatedParams, onChangeAndRun);

  // ORDER BY can reference any dimension or any aggregation (by its result-column alias).
  const orderByColumns: ComplexField[] = [
    ...savedParams.dimensions,
    ...savedParams.aggregations
      .filter(TableBuilder.isValidAggregation)
      .map((aggregation) => ({ name: TableBuilder.aggregationLabelOf(aggregation) })),
  ];

  return (
    <>
      <SelectTable
        options={resources.tables}
        selected={savedParams.tableName}
        isLoading={resources.isTablesLoading}
        onChange={(tableName) => onChange({ ...savedParams, tableName })}
      />
      <SelectTimeColumn
        selected={savedParams.timeColumn}
        timeColumns={resources.timeColumns}
        isLoading={resources.isColumnsLoading}
        onChange={(timeColumn) => onChangeAndRun({ ...savedParams, timeColumn })}
      />
      <SelectGroupBy
        selected={savedParams.dimensions}
        columns={resources.dimensionColumns}
        disabled={false}
        isLoading={resources.isColumnsLoading}
        onChange={(dimensions) => onChangeAndRun({ ...savedParams, dimensions })}
      />
      <SelectAggregations
        aggregations={savedParams.aggregations}
        columns={resources.aggregationColumns}
        isLoadingColumns={resources.isColumnsLoading}
        onChange={(aggregations) => onChangeAndRun({ ...savedParams, aggregations })}
      />
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
      <SelectOrderBy
        selected={savedParams.orderBy}
        columns={orderByColumns}
        disabled={false}
        onChange={(orderBy) => onChangeAndRun({ ...savedParams, orderBy })}
      />
      <SelectQueryOptions
        selected={savedParams.queryOptions}
        onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
      />
      <InputLimit current={savedParams.limit} onChange={(limit) => onChangeAndRun({ ...savedParams, limit })} />
      <SqlPreview sql={resources.sqlPreview} />
    </>
  );
}
