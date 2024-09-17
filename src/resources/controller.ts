import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';

export function useTables(datasource: DataSource): string[] | undefined {
  const [tables, setTables] = useState<string[] | undefined>();

  useEffect(() => {
    listTables(datasource).then((tables) => setTables(tables));
  }, [datasource]);

  return tables;
}

interface ListTablesResponse extends PinotResourceResponse {
  tables: string[] | null;
}

export async function listTables(datasource: DataSource): Promise<string[]> {
  const endpoint = 'tables';
  return fetchControllerResource<ListTablesResponse>(datasource, endpoint).then((resp) => resp.tables || []);
}

interface GetTableSchemaResponse extends PinotResourceResponse {
  schema: TableSchema | null;
}

export function useTableSchema(datasource: DataSource, tableName: string | undefined): TableSchema | undefined {
  const [tableSchema, setTableSchema] = useState<TableSchema | undefined>(undefined);

  useEffect(() => {
    if (tableName) {
      fetchTableSchema(datasource, tableName).then((schema) => setTableSchema(schema || undefined));
    }
  }, [datasource, tableName]);

  return tableSchema;
}

export async function fetchTableSchema(datasource: DataSource, tableName: string): Promise<TableSchema | null> {
  const endpoint = 'tables/' + tableName + '/schema';
  return fetchControllerResource<GetTableSchemaResponse>(datasource, endpoint).then((resp) => resp.schema);
}

async function fetchControllerResource<T>(datasource: DataSource, endpoint: string): Promise<T> {
  return datasource.getResource<T>(endpoint);
}
