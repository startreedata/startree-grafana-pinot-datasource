import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';

interface GetDatabasesResponse {
  databases: string[] | null;
  error: string | null;
}

export function useDatabases(datasource: DataSource): string[] | undefined {
  const resp = useControllerResource<GetDatabasesResponse>(datasource, undefined, 'databases');
  return resp?.databases || undefined;
}

interface GetTablesResponse {
  tables: string[] | null;
  error: string | null;
}

export function useTables(datasource: DataSource, databaseName: string | undefined): string[] | undefined {
  const resp = useControllerResource<GetTablesResponse>(datasource, databaseName, 'tables');
  return resp?.tables || undefined;
}

interface GetTableSchemaResponse {
  schema: TableSchema | null;
  error: string | null;
}

export function useTableSchema(
  datasource: DataSource,
  databaseName: string | undefined,
  tableName: string | undefined
): TableSchema | undefined {
  const noop = !tableName;
  const resp = useControllerResource<GetTableSchemaResponse>(
    datasource,
    databaseName,
    'tables/' + tableName + '/schema',
    noop
  );
  return resp?.schema || undefined;
}

function useControllerResource<T>(
  datasource: DataSource,
  databaseName: string | undefined,
  endpoint: string,
  noop?: boolean
): T | undefined {
  const [resp, setResp] = useState<T | undefined>(undefined);

  const params = new URLSearchParams();
  if (databaseName) {
    params.set('database', databaseName);
  }

  const path = `${endpoint}?${params.toString()}`;
  useEffect(() => {
    if (noop) {
      return;
    }
    datasource
      .getResource<T>(path)
      .then((resp) => setResp(resp))
  }, [datasource, databaseName, path, noop]);
  return resp;
}
