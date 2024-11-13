import { FormLabel } from './FormLabel';
import React from 'react';
import allLabels from '../../labels';

export function SqlPreview({ sql }: { sql: string }) {
  const labels = allLabels.components.QueryEditor.sqlPreview;

  return (
    <>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <pre data-testid="sql-preview-value">{sql}</pre>
    </>
  );
}
