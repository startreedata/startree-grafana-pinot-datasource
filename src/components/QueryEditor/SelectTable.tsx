import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTable(props: { selected: string; options: string[]; onChange: (val: string) => void }) {
  const { selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.table;

  if (options && options?.length === 1 && selected !== options[0]) {
    onChange(options[0]);
  } else if (options && selected && !options.includes(selected)) {
    onChange('');
  }

  return (
    <>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        invalid={!selected}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected || null}
        onChange={(change) => onChange(change.value || '')}
      />
    </>
  );
}
