import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';

export interface Column {
  name: string;
  key: string;
}

export interface ListDimensionColumnsRequest {
  tableName: string | undefined;
  timeColumn: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  filters: DimensionFilter[];
}

export function useDimensionColumns(datasource: DataSource, request: ListDimensionColumnsRequest): Column[] {
  const [cols, setCols] = useState<Column[]>([]);
  useEffect(() => {
    listDimensionColumns(datasource, request).then((res) => setCols(res));
  }, [datasource, JSON.stringify(request)]); // eslint-disable-line react-hooks/exhaustive-deps
  return cols;
}

export async function listDimensionColumns(
  datasource: DataSource,
  request: ListDimensionColumnsRequest
): Promise<Column[]> {
  return datasource
    .postResource<PinotResourceResponse<Column[]>>('columns/dimension', request)
    .then((resp) => resp.result || [])
    .catch(() => []);
}
