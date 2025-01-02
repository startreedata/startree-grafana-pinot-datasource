import React from 'react';
import { InlineField, Input } from '@grafana/ui';
import allLabels from '../../labels';

export function InputDatabase(props: { value: string | undefined; onChange: (val: string | undefined) => void }) {
  const { value, onChange } = props;
  const labels = allLabels.components.ConfigEditor.database;

  return (
    <InlineField
      data-testid="input-database"
      label={labels.label}
      labelWidth={24}
      tooltip={labels.tooltip}
      grow
      interactive
    >
      <Input
        width={40}
        onChange={(event) => onChange(event.currentTarget.value || undefined)}
        value={value}
        placeholder={labels.placeholder}
      />
    </InlineField>
  );
}
