import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTable(props: {
  selected: string | undefined;
  options: string[] | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.table;

  return (
    <>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        invalid={!selected}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </>
  );
}
