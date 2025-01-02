import { useTables } from '../resources/controller';
import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { useEffect, useState } from 'react';
import { previewSqlCode, PreviewSqlCodeRequest } from '../resources/previewSql';
import { CodeParams } from './codeParams';
import { UseResourceResult } from '../resources/UseResourceResult';

interface CodeResources {
  tables: string[];
  isTablesLoading: boolean;
  sqlPreview: string;
  isSqlPreviewLoading: boolean;
}

export function useCodeResources(
  datasource: DataSource,
  timeRange: { to: DateTime | undefined; from: DateTime | undefined },
  intervalSize: string | undefined,
  interpolatedParams: CodeParams
): CodeResources {
  const { result: tables, loading: isTablesLoading } = useTables(datasource);
  const { result: sqlPreview, loading: isSqlPreviewLoading } = useSqlPreview(
    datasource,
    intervalSize,
    timeRange,
    interpolatedParams
  );
  return {
    tables,
    isTablesLoading,
    sqlPreview,
    isSqlPreviewLoading,
  };
}

function useSqlPreview(
  datasource: DataSource,
  intervalSize: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  interpolatedParams: CodeParams
): UseResourceResult<string> {
  const [result, setResult] = useState('');
  const [loading, setLoading] = useState(false);

  const previewRequest: PreviewSqlCodeRequest = {
    intervalSize: intervalSize,
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    tableName: interpolatedParams.tableName,
    timeColumnAlias: interpolatedParams.timeColumnAlias,
    metricColumnAlias: interpolatedParams.metricColumnAlias,
    code: interpolatedParams.pinotQlCode,
  };

  useEffect(() => {
    setLoading(true);
    previewSqlCode(datasource, previewRequest)
      .then((val) => val && setResult(val))
      .finally(() => setLoading(false));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps

  return { result, loading };
}
