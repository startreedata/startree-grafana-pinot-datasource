import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';

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

export function useDistinctValues(datasource: DataSource, request: DistinctValuesRequest): string[] | undefined {
  const [data, setData] = useState<string[]>([]);
  useEffect(() => {
    // TODO: No need to make the request until all fields are present.
    datasource.postResource<DistinctValuesResponse>('distinctValues', request).then((resp) => setData(resp.valueExprs));
  }, [JSON.stringify(request)]);
  return data;
}
