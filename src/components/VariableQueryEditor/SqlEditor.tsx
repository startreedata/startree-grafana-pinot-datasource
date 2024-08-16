import { InlineFormLabel } from '@grafana/ui';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import React from 'react';
import allLabels from '../../labels';

export function SqlEditor(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.VariableQueryEditor.sqlEditor;

  return (
    <>
      <InlineFormLabel width={10} tooltip={labels.tooltip}>
        {labels.label}
      </InlineFormLabel>
      <div style={{ flex: '1 1 auto' }}>
        <GrafanaSqlEditor query={current || ''} onChange={(val) => onChange(val)} />
      </div>
    </>
  );
}
