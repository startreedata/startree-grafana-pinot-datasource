import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';

interface GetDatabasesResponse {
  databases: string[];
}

export function useDatabases(datasource: DataSource): string[] | undefined {
  const resp = useControllerResource<GetDatabasesResponse>(datasource, undefined, 'databases');
  return resp?.databases;
}

interface GetTablesResponse {
  tables: string[];
}

export function useTables(datasource: DataSource, databaseName: string | undefined): string[] | undefined {
  const resp = useControllerResource<GetTablesResponse>(datasource, databaseName, 'tables');
  return resp?.tables;
}

interface GetTableSchemaResponse {
  schema: TableSchema;
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
  return resp?.schema;
}

function useControllerResource<T>(
  datasource: DataSource,
  databaseName: string | undefined,
  path: string,
  noop?: boolean
): T | undefined {
  const [resp, setResp] = useState<T | undefined>(undefined);

  const params = new URLSearchParams();
  if (databaseName) {
    params.set('database', databaseName);
  }

  useEffect(() => {
    if (noop) return;
    datasource.getResource<T>(`${path}?${params.toString()}`).then((resp) => setResp(resp));
  }, [databaseName, path]);
  return resp;
}
