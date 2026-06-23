import { ComplexField } from '../dataquery/ComplexField';
import { Aggregation } from '../dataquery/Aggregation';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { OrderByClause } from '../dataquery/OrderByClause';
import { QueryOption } from '../dataquery/QueryOption';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { AggregationFunction } from '../components/QueryEditor/SelectAggregation';
import { Column, useColumns } from '../resources/columns';
import { isEmpty } from 'lodash';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { useEffect, useState } from 'react';
import { previewTableSql, PreviewTableSqlRequest } from '../resources/previewSql';
import { DisplayType } from '../dataquery/DisplayType';
import { columnLabelOf } from './complexField';

export interface Params {
  tableName: string;
  timeColumn: string;
  dimensions: ComplexField[];
  aggregations: Aggregation[];
  limit: number;
  filters: DimensionFilter[];
  orderBy: OrderByClause[];
  queryOptions: QueryOption[];
}

export interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  timeColumns: Column[];
  dimensionColumns: Column[];
  aggregationColumns: Column[];
  filterColumns: Column[];
  isColumnsLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    timeColumn: query.timeColumn || '',
    dimensions: dimensionsFrom(query),
    aggregations: query.aggregations || [],
    limit: query.limit || 0,
    filters: query.filters || [],
    orderBy: query.orderBy || [],
    queryOptions: query.queryOptions || [],
  };
}

function dimensionsFrom(query: PinotDataQuery): ComplexField[] {
  return (query.groupByColumns || []).map<ComplexField>((col) => ({ name: col })).concat(query.groupByColumnsV2 || []);
}

/** Result column name of an aggregation. Must match the Go aggregationAlias so ORDER BY lines up. */
export function aggregationLabelOf(aggregation: Aggregation): string {
  const column = columnLabelOf(aggregation.column?.name, aggregation.column?.key);
  const arg = !column && aggregation.function === AggregationFunction.COUNT ? '*' : column;
  return `${aggregation.function}(${arg})`;
}

export function isValidAggregation(aggregation: Aggregation): boolean {
  return Boolean(aggregation.function) && (Boolean(aggregation.column?.name) || aggregation.function === AggregationFunction.COUNT);
}

export function canRunQuery(params: Params): boolean {
  switch (true) {
    case !params.tableName:
    case !params.timeColumn:
    case isEmpty(params.dimensions) && !params.aggregations.some(isValidAggregation):
      return false;
    default:
      return true;
  }
}

export function applyDefaults(params: Params, resources: { timeColumns: Column[] }): boolean {
  let changed = false;

  const timeColumnCandidates = resources.timeColumns.filter((t) => !t.isDerived);
  if (!params.timeColumn && timeColumnCandidates.length > 0) {
    changed = true;
    params.timeColumn = timeColumnCandidates[0].name;
  }

  if (isEmpty(params.dimensions) && isEmpty(params.aggregations)) {
    changed = true;
    params.aggregations = [{ function: AggregationFunction.COUNT, column: {} }];
  }

  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotQL,
    editorMode: EditorMode.Builder,
    displayType: DisplayType.TABLE,
    tableName: params.tableName || undefined,
    timeColumn: params.timeColumn || undefined,
    groupByColumns: undefined,
    groupByColumnsV2: isEmpty(params.dimensions) ? undefined : params.dimensions,
    aggregations: isEmpty(params.aggregations) ? undefined : params.aggregations,
    filters: isEmpty(params.filters) ? undefined : params.filters,
    orderBy: isEmpty(params.orderBy) ? undefined : params.orderBy,
    queryOptions: isEmpty(params.queryOptions) ? undefined : params.queryOptions,
    limit: params.limit || undefined,
  };
}

export function useResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  interpolatedParams: Params
): Resources {
  const tablesResult = useTables(datasource);

  const columnsResult = useColumns(datasource, {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    filters: interpolatedParams.filters,
  });

  const sqlPreviewResult = useSqlPreview(datasource, timeRange, interpolatedParams);
  return resourcesFrom(tablesResult, columnsResult, sqlPreviewResult);
}

export function resourcesFrom(
  tablesResult: UseResourceResult<string[]>,
  columnsResult: UseResourceResult<Column[]>,
  sqlPreviewResult: UseResourceResult<string>
): Resources {
  const { result: tables, loading: isTablesLoading } = tablesResult;
  const { result: columns, loading: isColumnsLoading } = columnsResult;
  const { result: sqlPreview, loading: isSqlPreviewLoading } = sqlPreviewResult;
  return {
    tables,
    isTablesLoading,
    columns,
    timeColumns: columns.filter(({ isTime, isDerived }) => isTime && !isDerived),
    dimensionColumns: columns.filter(({ isTime }) => !isTime),
    aggregationColumns: columns.filter(({ isTime, isMetric }) => !isTime && isMetric),
    filterColumns: columns.filter(({ isTime }) => !isTime),
    isColumnsLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  interpolatedParams: Params
): UseResourceResult<string> {
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);

  const previewRequest: PreviewTableSqlRequest = {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    dimensions: interpolatedParams.dimensions,
    aggregations: interpolatedParams.aggregations,
    filters: interpolatedParams.filters,
    orderBy: interpolatedParams.orderBy,
    queryOptions: interpolatedParams.queryOptions,
    limit: interpolatedParams.limit,
  };

  useEffect(() => {
    setLoading(true);
    previewTableSql(datasource, previewRequest)
      .then((val) => val && setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { result, loading };
}
