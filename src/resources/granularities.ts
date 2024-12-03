import { PinotResourceResponse } from './PinotResourceResponse';
import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';

export interface ListGranularitiesRequest {
  tableName: string | undefined;
  timeColumn: string | undefined;
}

export interface Granularity {
  name: string;
  derived: boolean;
  seconds: number;
}

export interface ListGranularitiesResponse extends PinotResourceResponse {
  granularities: Granularity[];
}

const CommonGranularities = [
  { name: 'auto', derived: false, seconds: 0 },
  { name: 'MILLISECONDS', derived: false, seconds: 0.001 },
  { name: 'SECONDS', derived: false, seconds: 1 },
  { name: 'MINUTES', derived: false, seconds: 60 },
  { name: 'HOURS', derived: false, seconds: 3600 },
  { name: 'DAYS', derived: false, seconds: 86400 },
];

export function useGranularities(
  datasource: DataSource,
  tableName: string | undefined,
  timeColumn: string | undefined
): Granularity[] {
  const [granularities, setGranularities] = useState<Granularity[]>(CommonGranularities);

  useEffect(() => {
    listGranularities(datasource, { tableName, timeColumn }).then((granularities) => setGranularities(granularities));
  }, [datasource, tableName, timeColumn]);

  return granularities;
}

export async function listGranularities(
  datasource: DataSource,
  request: ListGranularitiesRequest
): Promise<Granularity[]> {
  return datasource
    .postResource<ListGranularitiesResponse>('granularities', request)
    .then((resp) => resp.granularities || CommonGranularities)
    .catch(() => CommonGranularities);
}
