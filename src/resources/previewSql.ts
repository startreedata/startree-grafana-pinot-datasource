import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import { OrderByClause } from '../types/OrderByClause';
import { QueryOption } from '../types/QueryOption';
import { PinotResourceResponse } from './PinotResourceResponse';
import { QueryDistinctValuesRequest } from './distinctValues';
import { DateTime } from '@grafana/data';
import { ComplexField } from '../types/ComplexField';
import { JsonExtractor, RegexpExtractor } from '../types/PinotDataQuery';

type PreviewSqlResponse = PinotResourceResponse<string>;

export interface PreviewSqlBuilderRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  metricColumn: ComplexField | undefined;
  groupByColumns: ComplexField[] | undefined;
  aggregationFunction: string | undefined;
  filters: DimensionFilter[] | undefined;
  limit: number | undefined;
  granularity: string | undefined;
  orderBy: OrderByClause[] | undefined;
  queryOptions: QueryOption[] | undefined;
  expandMacros: boolean;
}

export async function previewSqlBuilder(datasource: DataSource, request: PreviewSqlBuilderRequest): Promise<string> {
  if (
    request.intervalSize &&
    request.tableName &&
    request.timeColumn &&
    (request.metricColumn || request.aggregationFunction === 'COUNT') &&
    request.aggregationFunction &&
    request.timeRange.to &&
    request.timeRange.from
  ) {
    return datasource
      .postResource<PreviewSqlResponse>('preview/sql/builder', request)
      .then((resp) => resp.result || '')
      .catch(() => '');
  } else {
    return '';
  }
}

export interface PreviewLogsSqlRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  tableName: string | undefined;
  timeColumn: string | undefined;
  logColumn: ComplexField | undefined;
  logColumnAlias: string | undefined;
  metadataColumns: ComplexField[] | undefined;
  jsonExtractors: JsonExtractor[] | undefined;
  regexpExtractors: RegexpExtractor[] | undefined;
  dimensionFilters: DimensionFilter[] | undefined;
  queryOptions: QueryOption[] | undefined;
  limit: number | undefined;
  expandMacros: boolean | undefined;
}

export async function previewLogsSql(datasource: DataSource, request: PreviewLogsSqlRequest): Promise<string> {
  if (request.tableName && request.timeColumn && request.timeRange.to && request.timeRange.from) {
    return datasource
      .postResource<PreviewSqlResponse>('preview/logs/sql', request)
      .then((resp) => resp.result || '')
      .catch(() => '');
  } else {
    return '';
  }
}

export interface PreviewSqlCodeRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  tableName: string | undefined;
  timeColumnAlias: string | undefined;
  timeColumnFormat: string | undefined;
  metricColumnAlias: string | undefined;
  code: string | undefined;
}

export async function previewSqlCode(datasource: DataSource, request: PreviewSqlCodeRequest): Promise<string> {
  console.log({ method: 'previewSqlCode', args: { datasource, request } });
  if (request.intervalSize && request.tableName && request.code) {
    return datasource
      .postResource<PreviewSqlResponse>('preview/sql/code', request)
      .then((resp) => resp.result || '')
      .catch(() => '');
  } else {
    return '';
  }
}

export type PreviewSqlDistinctValuesRequest = QueryDistinctValuesRequest;

export async function previewSqlDistinctValues(
  datasource: DataSource,
  request: PreviewSqlDistinctValuesRequest
): Promise<string> {
  if (request.tableName && request.columnName) {
    return datasource
      .postResource<PreviewSqlResponse>('preview/sql/distinctValues', request)
      .then((resp) => resp.result || '')
      .catch(() => '');
  } else {
    return '';
  }
}
