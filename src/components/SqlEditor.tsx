import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel } from '@grafana/ui';
import React from 'react';
import { SQLEditor } from '@grafana/experimental';
import { useTableSchema } from '../resources/resources';

export function SqlEditor(props: PinotQueryEditorProps) {
  const { query, datasource, onChange } = props;

  const tableSchema = useTableSchema(datasource, query.databaseName, query.tableName);

  const timeCol = tableSchema?.dateTimeFieldSpecs.find((col) => col !== undefined)?.name;
  const metricCol = tableSchema?.metricFieldSpecs.find((col) => col !== undefined)?.name;

  const onChangeCode = (value: string) => onChange({ ...props.query, pinotQlCode: value });

  const defaultSql = `
SELECT 
  __timeGroup("${timeCol}") AS "time",
  SUM("${metricCol}") AS "metric"
FROM 
    __tableName 
WHERE 
    __timeFilter("${timeCol}")
GROUP BY
    __timeGroup("${timeCol}")
ORDER BY
    "time" DESC
LIMIT 1000000
`.trim();

  if (query.tableName && timeCol && metricCol && !query.pinotQlCode) {
    onChangeCode(defaultSql);
  }

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <div>
        <InlineFormLabel width={8} className="query-keyword" tooltip={'Sql Editor'}>
          Pinot Query
        </InlineFormLabel>
      </div>
      <div style={{ flex: '1 1 auto' }}>
        {query.tableName ? (
          <SQLEditor query={query.pinotQlCode || defaultSql} onChange={onChangeCode} />
        ) : (
          <pre>{'--- Select a table.'}</pre>
        )}
      </div>
    </div>
  );
}
