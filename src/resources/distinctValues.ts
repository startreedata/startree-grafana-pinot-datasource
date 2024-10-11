import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import { PinotResourceResponse } from './PinotResourceResponse';

export interface QueryDistinctValuesRequest {
  tableName: string | undefined;
  columnName: string | undefined;
  timeColumn?: string;
  timeRange?: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

interface QueryDistinctValuesResponse extends PinotResourceResponse {
  valueExprs: string[] | null;
}

export async function queryDistinctValuesForFilters(
  datasource: DataSource,
  request: QueryDistinctValuesRequest
): Promise<string[]> {
  if (request.tableName && request.columnName && request.timeRange && request.timeRange.to && request.timeRange.from) {
    return datasource
      .postResource<QueryDistinctValuesResponse>('query/distinctValues', request)
      .then((resp) => resp.valueExprs || [])
      .catch(() => []);
  } else {
    return [];
  }
}
