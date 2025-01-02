import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { DataSource } from '../datasource';
import { PinotResourceResponse } from './PinotResourceResponse';

export interface QueryDistinctValuesRequest {
  tableName: string | undefined;
  columnName: string | undefined;
  columnKey?: string;
  timeColumn?: string;
  timeRange?: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

export async function queryDistinctValuesForFilters(
  datasource: DataSource,
  request: QueryDistinctValuesRequest
): Promise<string[]> {
  type QueryDistinctValuesResponse = PinotResourceResponse<string[]>;

  if (request.tableName && request.columnName && request.timeRange && request.timeRange.to && request.timeRange.from) {
    return datasource
      .postResource<QueryDistinctValuesResponse>('query/distinctValues', request)
      .then((resp) => resp.result || [])
      .catch(() => []);
  } else {
    return [];
  }
}
