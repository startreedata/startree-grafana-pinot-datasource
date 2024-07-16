import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';

export interface SqlPreviewRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string;
  databaseName: string | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  metricColumn: string | undefined;
  groupByColumns: string[] | undefined;
  aggregationFunction: string | undefined;
  filters: DimensionFilter[] | undefined;
  limit: number | undefined;
  granularity: string | undefined;
}

interface SqlPreviewResponse {
  sql: string;
}

export async function fetchSqlPreview(datasource: DataSource, request: SqlPreviewRequest): Promise<string> {
  if (
    request.intervalSize &&
    request.intervalSize !== '0' &&
    request.databaseName &&
    request.tableName &&
    request.timeColumn &&
    request.metricColumn &&
    request.aggregationFunction &&
    request.timeRange.to &&
    request.timeRange.from
  ) {
    return datasource.postResource<SqlPreviewResponse>('preview', request).then((resp) => resp.sql);
  } else {
    return '';
  }
}
