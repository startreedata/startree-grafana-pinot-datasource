import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { CopyButton } from './CopyButton';

export function SqlPreview(props: { sql: string | undefined }) {
  const { sql } = props;
  const labels = allLabels.components.QueryEditor.sqlPreview;

  return (
    <div className="gf-form" data-testid="sql-preview-container">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ position: 'relative', display: 'flex', alignItems: 'flex-start' }}>
        <pre data-testid="sql-preview" style={{ margin: 0, flex: 1 }}>
          {sql || ''}
        </pre>
        {sql && <CopyButton text={sql} />}
      </div>
    </div>
  );
}
