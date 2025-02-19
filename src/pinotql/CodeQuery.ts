import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { DisplayType } from '../dataquery/DisplayType';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { useEffect, useState } from 'react';
import { previewSqlCode, PreviewSqlCodeRequest } from '../resources/previewSql';
import { columnLabelOf } from '../dataquery/ComplexField'; //language=text
import { Params as TimeSeriesBuilderParams } from './TimeSeriesBuilder';
import { Params as LogsBuilderParams } from './LogsBuilder'; //language=text

//language=text
export const DefaultQuerySql = `SELECT $__timeGroup("timestamp") AS $__timeAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 100000`;

export interface Params {
  displayType: string;
  tableName: string;
  pinotQlCode: string;
  timeColumnAlias: string;
  metricColumnAlias: string;
  logColumnAlias: string;
  legend: string;
  seriesLimit: number;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    displayType: query.displayType || '',
    tableName: query.tableName || '',
    pinotQlCode: query.pinotQlCode || '',
    timeColumnAlias: query.timeColumnAlias || '',
    metricColumnAlias: query.metricColumnAlias || '',
    logColumnAlias: query.logColumnAlias || '',
    legend: query.legend || '',
    seriesLimit: query.seriesLimit || 0,
  };
}

export function paramsFromTimeSeriesBuilder(params: TimeSeriesBuilderParams, sql: string): Params {
  return {
    displayType: DisplayType.TIMESERIES,
    tableName: params.tableName,
    metricColumnAlias: columnLabelOf(params.metricColumn.name, params.metricColumn.key),
    pinotQlCode: sql,
    legend: params.legend,
    timeColumnAlias: '',
    logColumnAlias: '',
    seriesLimit: params.seriesLimit,
  };
}

export function paramsFromLogsBuilder(params: LogsBuilderParams, sql: string): Params {
  return {
    displayType: DisplayType.LOGS,
    tableName: params.tableName,
    logColumnAlias: columnLabelOf(params.logColumn.name, params.logColumn.key),
    pinotQlCode: sql,
    timeColumnAlias: '',
    metricColumnAlias: '',
    legend: '',
    seriesLimit: 0,
  };
}

export function applyDefaults(params: Params): boolean {
  let changed = false;
  if (!params.displayType) {
    changed = true;
    params.displayType = DisplayType.TIMESERIES;
  }
  if (!params.pinotQlCode) {
    changed = true;
    params.pinotQlCode = DefaultQuerySql;
  }
  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotQL,
    editorMode: EditorMode.Code,
    displayType: params.displayType || undefined,
    tableName: params.tableName || undefined,
    pinotQlCode: params.pinotQlCode || undefined,
    timeColumnAlias: params.timeColumnAlias || undefined,
    metricColumnAlias: params.metricColumnAlias || undefined,
    logColumnAlias: params.logColumnAlias || undefined,
    legend: params.legend || undefined,
    seriesLimit: params.seriesLimit || undefined,
  };
}

interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function useResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  intervalSize: string | undefined,
  interpolatedParams: Params
): Resources {
  const { result: tables, loading: isTablesLoading } = useTables(datasource);
  const { result: sqlPreview, loading: isSqlPreviewLoading } = useSqlPreview(
    datasource,
    intervalSize,
    timeRange,
    interpolatedParams
  );
  return {
    tables,
    isTablesLoading,
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

  const previewRequest: PreviewSqlCodeRequest = {
    intervalSize: intervalSize,
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    tableName: interpolatedParams.tableName,
    timeColumnAlias: interpolatedParams.timeColumnAlias,
    metricColumnAlias: interpolatedParams.metricColumnAlias,
    code: interpolatedParams.pinotQlCode,
  };

  useEffect(() => {
    setLoading(true);
    previewSqlCode(datasource, previewRequest)
      .then((val) => val && setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps

  return { result, loading };
}
