import { DateTime } from '@grafana/data';
import { DataSource } from '../datasource';

export interface SqlCodePreviewRequest {
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string;
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
    request.tableName &&
    request.timeColumnAlias &&
    request.metricColumnAlias &&
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
