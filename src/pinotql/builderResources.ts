import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { Granularity, useGranularities } from '../resources/granularities';
import { Column, useColumns } from '../resources/columns';
import { UseResourceResult } from '../resources/UseResourceResult';
import { useTables } from '../resources/controller';
import { BuilderParams } from './builderParams';
import { useEffect, useState } from 'react';
import { previewSqlBuilder, PreviewSqlBuilderRequest } from '../resources/previewSql';

export interface BuilderResources {
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

export function useBuilderResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  intervalSize: string | undefined,
  interpolatedParams: BuilderParams
): BuilderResources {
  const tablesResult = useTables(datasource);

  const columnsResult = useColumns(datasource, {
    timeRange: timeRange,
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    filters: interpolatedParams.filters,
  });

  const granularitiesResult = useGranularities(datasource, interpolatedParams.tableName, interpolatedParams.timeColumn);
  const sqlPreviewResult = useSqlPreview(datasource, intervalSize, timeRange, interpolatedParams);
  return builderResourcesFrom(tablesResult, columnsResult, granularitiesResult, sqlPreviewResult);
}

export function builderResourcesFrom(
  tablesResult: UseResourceResult<string[]>,
  columnsResult: UseResourceResult<Column[]>,
  granularitiesResult: UseResourceResult<Granularity[]>,
  sqlPreviewResult: UseResourceResult<string>
): BuilderResources {
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
  interpolatedParams: BuilderParams
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
