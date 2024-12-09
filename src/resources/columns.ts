import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';
import { ComplexField } from '../types/ComplexField';

export interface ListDimensionColumnsRequest {
  tableName: string | undefined;
  timeColumn: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  filters: DimensionFilter[];
}

export function useDimensionColumns(datasource: DataSource, request: ListDimensionColumnsRequest): ComplexField[] {
  const [cols, setCols] = useState<ComplexField[]>([]);
  useEffect(() => {
    listDimensionColumns(datasource, request).then((res) => setCols(res));
  }, [datasource, JSON.stringify(request)]); // eslint-disable-line react-hooks/exhaustive-deps
  return cols;
}

export async function listDimensionColumns(
  datasource: DataSource,
  request: ListDimensionColumnsRequest
): Promise<ComplexField[]> {
  return datasource
    .postResource<PinotResourceResponse<ComplexField[]>>('columns/dimension', request)
    .then((resp) => resp.result || [])
    .catch(() => []);
}
