import { useSqlPreview } from '../resources/resources';
import React from 'react';
import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { FormLabel } from './FormLabel';

export function SqlPreview(props: PinotQueryEditorProps) {
  const { data, range, query, datasource } = props;

  const sql = useSqlPreview(datasource, {
    aggregationFunction: query.aggregationFunction,
    databaseName: query.databaseName,
    groupByColumns: query.groupByColumns,
    intervalSize: data?.request?.interval || '0',
    metricColumn: query.metricColumn,
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: { to: range?.to, from: range?.from },
    filters: query.filters,
  });

  return (
    <div className="gf-form">
      <FormLabel tooltip={'Sql Preview'} label={'Sql Preview'} />
      <pre>{sql}</pre>
    </div>
  );
}
