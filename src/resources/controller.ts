import { DataSource } from '../datasource';
import { TableSchema } from '../types/TableSchema';
import { useEffect, useState } from 'react';
import { PinotResourceResponse } from './PinotResourceResponse';
import {UseResourceResult} from "./UseResourceResult";


export function useTables(datasource: DataSource): string[] | undefined {
  const [tables, setTables] = useState<string[] | undefined>();

  useEffect(() => {
    listTables(datasource).then((tables) => setTables(tables));
  }, [datasource]);

  return tables;
}

export async function listTables(datasource: DataSource): Promise<string[]> {
  const endpoint = 'tables';
  type ListTablesResponse = PinotResourceResponse<string[]>;
  return fetchControllerResource<ListTablesResponse>(datasource, endpoint).then((resp) => resp.result || []);
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
  type GetTableSchemaResponse = PinotResourceResponse<TableSchema>;
  return fetchControllerResource<GetTableSchemaResponse>(datasource, endpoint).then((resp) => resp.result);
}

export interface TimeColumn {
  name: string;
  isDerived: boolean;
  hasDerivedGranularities: boolean;
}

export function useTableTimeColumns(
  datasource: DataSource,
  tableName: string | undefined
): UseResourceResult<TimeColumn[]> {
  const [result, setResult] = useState<TimeColumn[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    if (tableName) {
      setLoading(true);
      listTableTimeColumns(datasource, tableName)
        .then((res) => setResult(res))
        .finally(() => setLoading(false));
    }
  }, [datasource, tableName]);

  return { loading, result };
}

export async function listTableTimeColumns(datasource: DataSource, tableName: string): Promise<TimeColumn[]> {
  const endpoint = 'tables/' + tableName + '/timeColumns';
  type ListTableTimeColumnsResponse = PinotResourceResponse<TimeColumn[]>;
  return fetchControllerResource<ListTableTimeColumnsResponse>(datasource, endpoint).then((resp) => resp.result || []);
}

async function fetchControllerResource<T>(datasource: DataSource, endpoint: string): Promise<T> {
  return datasource.getResource<T>(endpoint);
}
