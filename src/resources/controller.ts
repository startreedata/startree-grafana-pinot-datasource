import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';

interface GetTablesResponse extends PinotResourceResponse {
  tables: string[] | null;
}

export function useTables(datasource: DataSource): string[] | undefined {
  const [tables, setTables] = useState<string[] | undefined>();

  useEffect(() => {
    fetchTables(datasource).then((resp) => setTables(resp.tables || undefined));
  }, [datasource]);

  return tables;
}

export async function fetchTables(datasource: DataSource): Promise<GetTablesResponse> {
  return fetchControllerResource<GetTablesResponse>(datasource, 'tables');
}

interface GetTableSchemaResponse extends PinotResourceResponse {
  schema: TableSchema | null;
}

export function useTableSchema(datasource: DataSource, tableName: string | undefined): TableSchema | undefined {
  const [tableSchema, setTableSchema] = useState<TableSchema | undefined>(undefined);

  useEffect(() => {
    if (tableName) {
      fetchTableSchema(datasource, tableName).then((resp) => setTableSchema(resp.schema || undefined));
    }
  }, [datasource, tableName]);

  return tableSchema;
}

export async function fetchTableSchema(datasource: DataSource, tableName: string): Promise<GetTableSchemaResponse> {
  return fetchControllerResource<GetTableSchemaResponse>(datasource, 'tables/' + tableName + '/schema');
}

async function fetchControllerResource<T>(datasource: DataSource, endpoint: string): Promise<T> {
  return datasource.getResource<T>(endpoint);
}
