import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';

interface GetDatabasesResponse {
  databases: string[] | null;
  error: string | null;
}

export function useDatabases(datasource: DataSource): string[] | undefined {
  const resp = useControllerResource<GetDatabasesResponse>(datasource, 'databases');
  return resp?.databases || undefined;
}

interface GetTablesResponse {
  tables: string[] | null;
  error: string | null;
}

export function useTables(datasource: DataSource): string[] | undefined {
  const resp = useControllerResource<GetTablesResponse>(datasource, 'tables');
  return resp?.tables || undefined;
}

interface GetTableSchemaResponse {
  schema: TableSchema | null;
  error: string | null;
}

export function useTableSchema(datasource: DataSource, tableName: string | undefined): TableSchema | undefined {
  const noop = !tableName;
  const resp = useControllerResource<GetTableSchemaResponse>(datasource, 'tables/' + tableName + '/schema', noop);
  return resp?.schema || undefined;
}

function useControllerResource<T>(datasource: DataSource, endpoint: string, noop?: boolean): T | undefined {
  const [resp, setResp] = useState<T | undefined>(undefined);

  useEffect(() => {
    if (noop) {
      return;
    }
    datasource.getResource<T>(endpoint).then((resp) => setResp(resp));
  }, [datasource, endpoint, noop]);
  return resp;
}
