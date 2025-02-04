import { ComplexField } from '../dataquery/ComplexField';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { OrderByClause } from '../dataquery/OrderByClause';
import { QueryOption } from '../dataquery/QueryOption';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { AggregationFunction } from '../components/QueryEditor/SelectAggregation';
import { Column, useColumns } from '../resources/columns';
import { isEmpty } from 'lodash';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { Granularity, useGranularities } from '../resources/granularities';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { useEffect, useState } from 'react';
import { previewSqlBuilder, PreviewSqlBuilderRequest } from '../resources/previewSql';
import { DisplayType } from '../dataquery/DisplayType';

export interface Params {
  tableName: string;
  timeColumn: string;
  metricColumn: ComplexField;
  granularity: string;
  aggregationFunction: string;
  limit: number;
  filters: DimensionFilter[];
  orderBy: OrderByClause[];
  queryOptions: QueryOption[];
  legend: string;
  groupByColumns: ComplexField[];
}

export interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  timeColumns: Column[];
  metricColumns: Column[];
  groupByColumns: Column[];
  filterColumns: Column[];
  isColumnsLoading: boolean;
  granularities: Granularity[];
  isGranularitiesLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    timeColumn: query.timeColumn || '',
    metricColumn: metricColumnFrom(query) || {},
    granularity: query.granularity || '',
    aggregationFunction: query.aggregationFunction || '',
    limit: query.limit || 0,
    filters: query.filters || [],
    orderBy: query.orderBy || [],
    queryOptions: query.queryOptions || [],
    legend: query.legend || '',
    groupByColumns: groupByColumnsFrom(query),
  };
}

function groupByColumnsFrom(query: PinotDataQuery): ComplexField[] {
  return (query.groupByColumns || []).map<ComplexField>((col) => ({ name: col })).concat(query.groupByColumnsV2 || []);
}

function metricColumnFrom(query: PinotDataQuery): ComplexField | undefined {
  if (query.metricColumnV2) {
    return query.metricColumnV2;
  } else if (query.metricColumn) {
    return { name: query.metricColumn };
  } else {
    return undefined;
  }
}

export function canRunQuery(params: Params): boolean {
  switch (true) {
    case !params.tableName:
    case !params.timeColumn:
    case !params.metricColumn.name && params.aggregationFunction !== AggregationFunction.COUNT:
      return false;
    default:
      return true;
  }
}

export function applyDefaults(
  params: Params,
  resources: {
    timeColumns: Column[];
    metricColumns: Column[];
  }
): boolean {
  let changed = false;

  const timeColumnCandidates = resources.timeColumns.filter((t) => !t.isDerived);
  if (!params.timeColumn && timeColumnCandidates.length > 0) {
    changed = true;
    params.timeColumn = timeColumnCandidates[0].name;
  }

  if (!params.metricColumn?.name && resources.metricColumns.length > 0) {
    changed = true;
    params.metricColumn = {
      name: resources.metricColumns[0].name,
      key: resources.metricColumns[0].key || undefined,
    };
  }

  if (!params.aggregationFunction) {
    changed = true;
    params.aggregationFunction = AggregationFunction.SUM;
  }

  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotQL,
    editorMode: EditorMode.Builder,
    displayType: DisplayType.TIMESERIES,
    tableName: params.tableName || undefined,
    timeColumn: params.timeColumn || undefined,
    metricColumn: undefined,
    metricColumnV2: params.metricColumn.name ? params.metricColumn : undefined,
    granularity: params.granularity || undefined,
    aggregationFunction: params.aggregationFunction || undefined,
    limit: params.limit || undefined,
    filters: isEmpty(params.filters) ? undefined : params.filters,
    orderBy: isEmpty(params.orderBy) ? undefined : params.orderBy,
    queryOptions: isEmpty(params.queryOptions) ? undefined : params.queryOptions,
    legend: params.legend || undefined,
    groupByColumns: undefined,
    groupByColumnsV2: isEmpty(params.groupByColumns) ? undefined : params.groupByColumns,
  };
}

export function useResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  intervalSize: string | undefined,
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

  const granularitiesResult = useGranularities(datasource, interpolatedParams.tableName, interpolatedParams.timeColumn);
  const sqlPreviewResult = useSqlPreview(datasource, intervalSize, timeRange, interpolatedParams);
  return resourcesFrom(tablesResult, columnsResult, granularitiesResult, sqlPreviewResult);
}

export function resourcesFrom(
  tablesResult: UseResourceResult<string[]>,
  columnsResult: UseResourceResult<Column[]>,
  granularitiesResult: UseResourceResult<Granularity[]>,
  sqlPreviewResult: UseResourceResult<string>
): Resources {
  const { result: tables, loading: isTablesLoading } = tablesResult;
  const { result: columns, loading: isColumnsLoading } = columnsResult;
  const { result: granularities, loading: isGranularitiesLoading } = granularitiesResult;
  const { result: sqlPreview, loading: isSqlPreviewLoading } = sqlPreviewResult;
  return {
    tables,
    isTablesLoading,
    columns,
    timeColumns: columns.filter(({ isTime, isDerived }) => isTime && !isDerived),
    metricColumns: columns.filter(({ isTime, isMetric }) => !isTime && isMetric),
    groupByColumns: columns.filter(({ isTime }) => !isTime),
    filterColumns: columns.filter(({ isTime }) => !isTime),
    isColumnsLoading,
    granularities,
    isGranularitiesLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(
  datasource: DataSource,
  intervalSize: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  interpolatedParams: Params
): UseResourceResult<string> {
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);

  const previewRequest: PreviewSqlBuilderRequest = {
    intervalSize: intervalSize,
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    aggregationFunction: interpolatedParams.aggregationFunction,
    groupByColumns: interpolatedParams.groupByColumns,
    metricColumn: interpolatedParams.metricColumn,
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    filters: interpolatedParams.filters,
    limit: interpolatedParams.limit,
    granularity: interpolatedParams.granularity,
    orderBy: interpolatedParams.orderBy,
    queryOptions: interpolatedParams.queryOptions,
  };

  useEffect(() => {
    setLoading(true);
    previewSqlBuilder(datasource, previewRequest)
      .then((val) => val && setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { result, loading };
}
