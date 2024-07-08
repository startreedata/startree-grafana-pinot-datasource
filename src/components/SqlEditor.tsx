import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import React from 'react';
import { SQLEditor } from '@grafana/experimental';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

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
  const labels = allLabels.components.QueryEditor.sqlEditor;

  const onChangeCode = (value: string) => onChange({ ...props.query, pinotQlCode: value });

  if (!query.pinotQlCode) {
    onChangeCode(DefaultSql);
  }

  return (
    <div className={'gf-form'}>
      <div>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
      </div>
      <div style={{ flex: '1 1 auto' }}>
        <SQLEditor query={query.pinotQlCode || ''} onChange={onChangeCode} />
      </div>
    </div>
  );
}
