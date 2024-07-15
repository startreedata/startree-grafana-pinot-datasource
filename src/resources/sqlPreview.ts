import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';

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
}

interface SqlPreviewResponse {
  sql: string;
}

export function useSqlPreview(datasource: DataSource, request: SqlPreviewRequest): string {
  const [data, setData] = useState<string>('');
  useEffect(() => {
    // TODO: No need to make the request until all fields are present.
    datasource.postResource<SqlPreviewResponse>('preview', request).then((resp) => setData(resp.sql));
  }, [datasource, request]);
  return data;
}
