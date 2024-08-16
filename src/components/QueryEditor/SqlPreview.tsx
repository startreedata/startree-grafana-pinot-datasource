import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { CopyButton } from './CopyButton';

export function SqlPreview(props: { sql: string | undefined }) {
  const { sql } = props;
  const labels = allLabels.components.QueryEditor.sqlPreview;

  return (
    <div className="gf-form">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <pre>
        {sql || ''}
        {sql && <CopyButton text={sql} />}
      </pre>
    </div>
  );
}
