import React from 'react';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

const DefaultSqlQuery = `
SELECT
  $__timeGroup("timestamp") AS $__timeAlias(),
  SUM("metric") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY $__timeGroup("timestamp")
ORDER BY $__timeAlias() DESC
LIMIT 1000000
`.trim();

export function SqlEditor(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.sqlEditor;

  if (current === undefined) {
    onChange(DefaultSqlQuery);
  }

  return (
    <div className={'gf-form'}>
      <div>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
      </div>
      <div style={{ flex: '1 1 auto' }}>
        <GrafanaSqlEditor query={current || ''} onChange={onChange} />
      </div>
    </div>
  );
}
