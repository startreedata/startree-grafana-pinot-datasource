import { ComplexField } from '../dataquery/ComplexField';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { QueryOption } from '../dataquery/QueryOption';
import { JsonExtractor } from '../dataquery/JsonExtractor';
import { RegexpExtractor } from '../dataquery/RegexpExtractor';
import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { EditorMode } from '../dataquery/EditorMode';
import { isEmpty } from 'lodash';
import { DisplayType } from '../dataquery/DisplayType';
import { Column, useColumns } from '../resources/columns';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { PinotDataType } from '../dataquery/PinotDataType';
import { useEffect, useState } from 'react';
import { previewLogsSql, PreviewLogsSqlRequest } from '../resources/previewSql';

export interface Params {
  tableName: string;
  timeColumn: string;
  limit: number;
  filters: DimensionFilter[];
  queryOptions: QueryOption[];
  logColumn: ComplexField;
  metadataColumns: ComplexField[];
  jsonExtractors: JsonExtractor[];
  regexpExtractors: RegexpExtractor[];
}

export interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  timeColumns: Column[];
  filterColumns: Column[];
  logMessageColumns: Column[];
  jsonExtractorColumns: Column[];
  regexpExtractorColumns: Column[];
  isColumnsLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    tableName: query.tableName || '',
    timeColumn: query.timeColumn || '',
    logColumn: query.logColumn || {},
    metadataColumns: query.metadataColumns || [],
    regexpExtractors: query.regexpExtractors || [],
    jsonExtractors: query.jsonExtractors || [],
    filters: query.filters || [],
    queryOptions: query.queryOptions || [],
    limit: query.limit || 0,
  };
}

export function canRunQuery(params: Params): boolean {
  switch (true) {
    case !params.tableName:
    case !params.timeColumn:
    case !params.logColumn.name:
      return false;
    default:
      return true;
  }
}

export function applyDefaults(
  params: Params,
  resources: {
    timeColumns: Column[];
    logMessageColumns: Column[];
  }
): boolean {
  let changed = false;

  const timeColumnCandidates = resources.timeColumns.filter((t) => !t.isDerived);
  if (!params.timeColumn && timeColumnCandidates.length > 0) {
    changed = true;
    params.timeColumn = timeColumnCandidates[0].name;
  }

  if (!params.logColumn?.name && resources.logMessageColumns.length > 0) {
    changed = true;
    params.logColumn = {
      name: resources.logMessageColumns[0].name,
      key: resources.logMessageColumns[0].key || undefined,
    };
  }
  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotQL,
    editorMode: EditorMode.Builder,
    displayType: DisplayType.LOGS,
    tableName: params.tableName || undefined,
    timeColumn: params.timeColumn || undefined,
    logColumn: params.logColumn.name ? params.logColumn : undefined,
    metadataColumns: isEmpty(params.metadataColumns) ? undefined : params.metadataColumns,
    regexpExtractors: isEmpty(params.regexpExtractors) ? undefined : params.regexpExtractors,
    jsonExtractors: isEmpty(params.jsonExtractors) ? undefined : params.jsonExtractors,
    filters: isEmpty(params.filters) ? undefined : params.filters,
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
    logMessageColumns: columns.filter(({ dataType }) => dataType === PinotDataType.STRING),
    filterColumns: columns.filter(({ isTime }) => !isTime),
    jsonExtractorColumns: columns.filter(({ dataType }) => [PinotDataType.STRING.toString(), PinotDataType.JSON.toString()].includes(dataType)),
    regexpExtractorColumns: columns.filter(({ dataType }) => [PinotDataType.STRING.toString()].includes(dataType)),
    isColumnsLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(
  datasource: DataSource,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  interpolatedParams: Params
): UseResourceResult<string> {
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);

  const previewRequest: PreviewLogsSqlRequest = {
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    expandMacros: true,
    tableName: interpolatedParams.tableName,
    timeColumn: interpolatedParams.timeColumn,
    limit: interpolatedParams.limit,
    queryOptions: interpolatedParams.queryOptions,
    logColumn: interpolatedParams.logColumn,
    metadataColumns: interpolatedParams.metadataColumns,
    jsonExtractors: interpolatedParams.jsonExtractors,
    regexpExtractors: interpolatedParams.regexpExtractors,
    filters: interpolatedParams.filters,
  };

  useEffect(() => {
    previewLogsSql(datasource, previewRequest)
      .then((val) => val && setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { result, loading };
}
