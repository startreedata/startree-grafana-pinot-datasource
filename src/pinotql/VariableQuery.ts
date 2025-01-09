import { PinotDataQuery } from '../dataquery/PinotDataQuery';
import { QueryType } from '../dataquery/QueryType';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { ColumnTypes } from '../components/VariableQueryEditor/SelectColumnType';
import { Column, useColumns } from '../resources/columns';
import { DataSource } from '../datasource';
import { useTables } from '../resources/tables';
import { UseResourceResult } from '../resources/UseResourceResult';
import { useEffect, useState } from 'react';
import { previewSqlDistinctValues } from '../resources/previewSql';

export interface Params {
  tableName: string;
  variableType: string;
  columnName: string;
  columnType: string;
  pinotQlCode: string;
}

export function paramsFrom(query: PinotDataQuery): Params {
  return {
    variableType: query.variableQuery?.variableType || '',
    tableName: query.tableName || '',
    columnName: query.variableQuery?.columnName || '',
    columnType: query.variableQuery?.columnType || '',
    pinotQlCode: query.variableQuery?.pinotQlCode || '',
  };
}

export function applyDefaults(params: Params): boolean {
  let changed = false;
  if (params.variableType === '') {
    changed = true;
    params.variableType = VariableType.TableList;
  }

  if (params.variableType === VariableType.ColumnList && params.columnType === '') {
    changed = true;
    params.columnType = ColumnTypes.All;
  }
  return changed;
}

export function dataQueryOf(query: PinotDataQuery, params: Params): PinotDataQuery {
  return {
    ...query,
    queryType: QueryType.PinotVariableQuery,
    tableName: params.tableName || undefined,
    variableQuery: {
      variableType: params.variableType || VariableType.TableList,
      columnName: params.columnName || undefined,
      columnType: params.columnType || ColumnTypes.All,
      pinotQlCode: params.pinotQlCode || undefined,
    },
  };
}

export interface Resources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  isColumnsLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function useResources(datasource: DataSource, interpolatedParams: Params): Resources {
  const { result: tables, loading: isTablesLoading } = useTables(datasource);
  const { result: columns, loading: isColumnsLoading } = useColumns(datasource, {
    tableName: interpolatedParams.tableName,
  });
  const { result: sqlPreview, loading: isSqlPreviewLoading } = useSqlPreview(datasource, interpolatedParams);

  return {
    tables,
    isTablesLoading,
    columns,
    isColumnsLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(datasource: DataSource, interpolatedParams: Params): UseResourceResult<string> {
  const [result, setResult] = useState<string>('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (interpolatedParams.variableType === VariableType.DistinctValues) {
      setLoading(true);
      previewSqlDistinctValues(datasource, {
        tableName: interpolatedParams.tableName,
        columnName: interpolatedParams.columnName,
      })
        .then((sqlPreview) => setResult(sqlPreview))
        .finally(() => setLoading(false));
    }
  }, [datasource, interpolatedParams.variableType, interpolatedParams.tableName, interpolatedParams.columnName]);

  return { result, loading };
}
