import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel } from '@grafana/ui';
import React from 'react';
import { SQLEditor } from '@grafana/experimental';

const DefaultSql = `
SELECT 
  $__timeGroup("timestamp") AS $__timeAlias(),
  SUM("metric") AS $__metricAlias()
FROM $__tableName()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 1000000
`.trim();

export function SqlEditor(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  const onChangeCode = (value: string) => onChange({ ...props.query, pinotQlCode: value });

  if (!query.pinotQlCode) {
    onChangeCode(DefaultSql);
  }

  return (
    <div className={'gf-form'}>
      <div>
        <InlineFormLabel width={8} className="query-keyword" tooltip={'Sql Editor'}>
          Pinot Query
        </InlineFormLabel>
      </div>
      <div style={{ flex: '1 1 auto' }}>
        <SQLEditor query={query.pinotQlCode || ''} onChange={onChangeCode} />
      </div>
    </div>
  );
}
