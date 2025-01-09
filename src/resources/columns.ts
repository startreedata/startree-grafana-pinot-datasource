import { DataSource } from '../datasource';
import { DateTime } from '@grafana/data';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';
import { UseResourceResult } from './UseResourceResult';
import { useTableSchema } from './tableSchema';
import { DimensionFilter } from '../dataquery/DimensionFilter';
import { isEmpty } from 'lodash';

export interface Column {
  name: string;
  key: string | null;
  dataType: string;
  isTime: boolean | null;
  isDerived: boolean | null;
  isMetric: boolean | null;
}

export interface ListColumnsRequest {
  tableName: string;
  timeColumn?: string;
  timeRange?: { to: DateTime | undefined; from: DateTime | undefined };
  filters?: DimensionFilter[];
}

export function useColumns(
  datasource: DataSource,
  { tableName, timeColumn, timeRange, filters }: ListColumnsRequest
): UseResourceResult<Column[]> {
  const [result, setResult] = useState<Column[]>([]);
  const [loading, setLoading] = useState(false);
  const { result: tableSchema } = useTableSchema(datasource, tableName);

  const request: ListColumnsRequest = {
    tableName: tableName,
  };

  if (!isEmpty(tableSchema?.complexFieldSpecs)) {
    request.timeColumn = timeColumn;
    request.timeRange = timeRange
      ? {
          to: timeRange.to?.endOf('second'),
          from: timeRange.from?.startOf('second'),
        }
      : undefined;
    request.filters = filters;
  }

  useEffect(() => {
    if (request.tableName) {
      setLoading(true);
      listColumns(datasource, request)
        .then((res) => setResult(res))
        .finally(() => setLoading(false));
    }
  }, [datasource, tableSchema, JSON.stringify(request)]); // eslint-disable-line react-hooks/exhaustive-deps
  return { loading, result };
}

export async function listColumns(datasource: DataSource, request: ListColumnsRequest): Promise<Column[]> {
  return datasource
    .postResource<PinotResourceResponse<Column[]>>('columns', request)
    .then((resp) => resp.result || [])
    .catch(() => []);
}
