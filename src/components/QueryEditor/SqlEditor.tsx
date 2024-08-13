import React from 'react';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SqlEditor(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.sqlEditor;

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
