import { PinotResourceResponse } from './PinotResourceResponse';
import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { UseResourceResult } from './UseResourceResult';

export interface ListGranularitiesRequest {
  tableName: string | undefined;
  timeColumn: string | undefined;
}

export interface Granularity {
  name: string;
  optimized: boolean;
  seconds: number;
}

const CommonGranularities: Granularity[] = [
  { name: 'auto', optimized: false, seconds: 0 },
  { name: 'MILLISECONDS', optimized: false, seconds: 0.001 },
  { name: 'SECONDS', optimized: false, seconds: 1 },
  { name: 'MINUTES', optimized: false, seconds: 60 },
  { name: 'HOURS', optimized: false, seconds: 3600 },
  { name: 'DAYS', optimized: false, seconds: 86400 },
];

export function useGranularities(
  datasource: DataSource,
  tableName: string | undefined,
  timeColumn: string | undefined
): UseResourceResult<Granularity[]> {
  const [result, setResult] = useState<Granularity[]>(CommonGranularities);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (tableName && timeColumn) {
      setLoading(true);
      listGranularities(datasource, {
        tableName,
        timeColumn,
      })
        .then((granularities) => setResult(granularities))
        .finally(() => setLoading(false));
    }
  }, [datasource, tableName, timeColumn]);

  return { loading, result };
}

export async function listGranularities(
  datasource: DataSource,
  request: ListGranularitiesRequest
): Promise<Granularity[]> {
  if (request.tableName && request.timeColumn) {
    type ListGranularitiesResponse = PinotResourceResponse<Granularity[]>;

    return datasource
      .postResource<ListGranularitiesResponse>('granularities', request)
      .then((resp) => resp.result || CommonGranularities)
      .catch(() => CommonGranularities);
  } else {
    return CommonGranularities;
  }
}
