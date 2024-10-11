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
      <pre data-testid="sql-preview">
        {sql || ''}
        {sql && <CopyButton text={sql} />}
      </pre>
    </div>
  );
}
