import { Column, useColumns } from '../resources/columns';
import { DataSource } from '../datasource';
import { useTables } from '../resources/controller';
import { VariableParams } from './variablePararms';
import { useEffect, useState } from 'react';
import { VariableType } from '../components/VariableQueryEditor/SelectVariableType';
import { previewSqlDistinctValues } from '../resources/previewSql';
import { UseResourceResult } from '../resources/UseResourceResult';

export interface VariableResources {
  tables: string[];
  isTablesLoading: boolean;
  columns: Column[];
  isColumnsLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function useVariableResources(datasource: DataSource, interpolatedParams: VariableParams): VariableResources {
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

function useSqlPreview(datasource: DataSource, interpolatedParams: VariableParams): UseResourceResult<string> {
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
