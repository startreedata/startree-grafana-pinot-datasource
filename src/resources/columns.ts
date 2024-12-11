import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { DimensionFilter } from '../types/DimensionFilter';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';
import { UseResourceResult } from './UseResourceResult';

export interface Column {
  name: string;
  key: string | null;
  dataType: string;
  isTime: boolean | null;
  isDerived: boolean | null;
  isMetric: boolean | null;
}

export interface ListColumnsRequest {
  tableName: string | undefined;
  timeColumn: string | undefined;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  filters: DimensionFilter[];
}

export function useColumns(datasource: DataSource, request: ListColumnsRequest): UseResourceResult<Column[]> {
  const [result, setResult] = useState<Column[]>([]);
  const [loading, setLoading] = useState(false);
  useEffect(() => {
    if (request.tableName) {
      setLoading(true);
      listColumns(datasource, request)
        .then((res) => setResult(res))
        .finally(() => setLoading(false));
    }
  }, [datasource, JSON.stringify(request)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { loading, result };
}

export async function listColumns(datasource: DataSource, request: ListColumnsRequest): Promise<Column[]> {
  return datasource
    .postResource<PinotResourceResponse<Column[]>>('columns', request)
    .then((resp) => resp.result || [])
    .catch(() => []);
}
