import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';

export interface DistinctValuesRequest {
  databaseName?: string;
  tableName?: string;
  columnName?: string;
  timeColumn?: string;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

interface DistinctValuesResponse {
  valueExprs: string[];
}

export async function fetchDistinctValues(datasource: DataSource, request: DistinctValuesRequest): Promise<string[]> {
  if (
    request.databaseName &&
    request.tableName &&
    request.columnName &&
    request.timeRange.to &&
    request.timeRange.from
  ) {
    return datasource.postResource<DistinctValuesResponse>('distinctValues', request).then((resp) => resp.valueExprs);
  } else {
    return [];
  }
}
