import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import {PinotResourceResponse, SqlPreviewResponse} from './PinotResourceResponse';

export interface DistinctValuesRequest {
  tableName: string | undefined;
  columnName: string | undefined;
  timeColumn?: string;
  timeRange?: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

interface DistinctValuesResponse extends PinotResourceResponse {
  valueExprs: string[] | null;
}

export async function fetchDistinctValuesForFilters(
  datasource: DataSource,
  request: DistinctValuesRequest
): Promise<string[]> {
  if (request.tableName && request.columnName && request.timeRange && request.timeRange.to && request.timeRange.from) {
    return datasource
      .postResource<DistinctValuesResponse>('distinctValues', request)
      .then((resp) => resp.valueExprs || [])
      .catch(() => []);
  } else {
    return [];
  }
}

export async function fetchDistinctValuesForVariables(
  datasource: DataSource,
  request: DistinctValuesRequest
): Promise<string[]> {
  if (request.tableName && request.columnName) {
    return datasource
      .postResource<DistinctValuesResponse>('distinctValues', request)
      .then((resp) => resp.valueExprs || [])
      .catch(() => []);
  } else {
    return [];
  }
}

export async function fetchDistinctValuesSqlPreview(
  datasource: DataSource,
  request: DistinctValuesRequest
): Promise<string> {
  if (request.tableName && request.columnName) {
    return datasource
      .postResource<SqlPreviewResponse>('distinctValuesSqlPreview', request)
      .then((resp) => resp.sql || '')
      .catch(() => '');
  } else {
    return '';
  }
}
