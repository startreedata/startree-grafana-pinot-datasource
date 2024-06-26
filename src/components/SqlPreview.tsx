import { useSqlPreview } from '../resources/resources';
import { InlineFormLabel } from '@grafana/ui';
import React from 'react';
import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';

export function SqlPreview(props: PinotQueryEditorProps) {
  const { data, range, query, datasource } = props;

  const sql = useSqlPreview(datasource, {
    aggregationFunction: query.aggregationFunction,
    databaseName: query.databaseName,
    dimensionColumns: query.dimensionColumns,
    intervalSize: data?.request?.interval || '0',
    metricColumn: query.metricColumn,
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: { to: range?.to, from: range?.from },
  });

  return (
    <div className="gf-form">
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Sql Preview'}>
        Sql Preview
      </InlineFormLabel>
      <pre>{sql}</pre>
    </div>
  );
}
