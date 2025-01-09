import { DataSource } from '../datasource';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';
import { UseResourceResult } from './UseResourceResult';

export function useTables(datasource: DataSource): UseResourceResult<string[]> {
  const [result, setResult] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    setLoading(true);
    listTables(datasource)
      .then((tables) => setResult(tables))
      .finally(() => setLoading(false));
  }, [datasource]);

  return { result, loading };
}

export async function listTables(datasource: DataSource): Promise<string[]> {
  const endpoint = 'tables';
  type ListTablesResponse = PinotResourceResponse<string[]>;
  return datasource.getResource<ListTablesResponse>(endpoint).then((resp) => resp.result || []);
}
