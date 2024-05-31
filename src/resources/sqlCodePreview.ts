import { DateTime } from '@grafana/data';
import { DataSource } from '../datasource';

export interface SqlCodePreviewRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string;
  databaseName: string | undefined;
  tableName: string | undefined;
  timeColumnAlias: string | undefined;
  timeColumnFormat: string | undefined;
  metricColumnAlias: string | undefined;
  code: string | undefined;
}

interface SqlCodePreviewResponse {
  sql: string | null;
  error: string | null;
}

export async function fetchSqlCodePreview(datasource: DataSource, request: SqlCodePreviewRequest): Promise<string> {
  if (
    request.intervalSize &&
    request.intervalSize !== '0' &&
    request.databaseName &&
    request.tableName &&
    request.timeColumnAlias &&
    request.metricColumnAlias &&
    request.timeColumnFormat &&
    request.code
  ) {
    return datasource
      .postResource<SqlCodePreviewResponse>('codePreview', request)
      .then((resp) => resp.sql || '')
      .catch(() => '');
  } else {
    return '';
  }
}
