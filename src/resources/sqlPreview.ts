import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import { OrderByClause } from '../types/OrderByClause';
import { QueryOption } from '../types/QueryOption';

export interface SqlPreviewRequest {
  timeRange: { to: string | undefined; from: string | undefined };
  intervalSize: string;
  tableName: string | undefined;
  timeColumn: string | undefined;
  metricColumn: string | undefined;
  groupByColumns: string[] | undefined;
  aggregationFunction: string | undefined;
  filters: DimensionFilter[] | undefined;
  limit: number | undefined;
  granularity: string | undefined;
  orderBy: OrderByClause[] | undefined;
  queryOptions: QueryOption[] | undefined;
}

interface SqlPreviewResponse {
  sql: string | null;
  error: string | null;
}

export async function fetchSqlBuilderPreview(datasource: DataSource, request: SqlPreviewRequest): Promise<string> {
  if (
    request.intervalSize &&
    request.intervalSize !== '0' &&
    request.tableName &&
    request.timeColumn &&
    (request.metricColumn || request.aggregationFunction === 'COUNT') &&
    request.aggregationFunction &&
    request.timeRange.to &&
    request.timeRange.from
  ) {
    return datasource
      .postResource<SqlPreviewResponse>('preview', request)
      .then((resp) => resp.sql || '')
      .catch(() => '');
  } else {
    return '';
  }
}
