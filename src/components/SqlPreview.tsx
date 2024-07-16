import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function SqlPreview(props: { sql: string | undefined }) {
  const { sql } = props;
  const labels = allLabels.components.QueryEditor.sqlPreview;

  return (
    <div className="gf-form">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <pre>{sql || ''}</pre>
    </div>
  );
}
