import allLabels from '../../labels';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectTable(props: {
  selected: string | undefined;
  options: string[] | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.table;

  return (
    <>
      <FormLabel label={labels.label} tooltip={labels.tooltip} />
      <Select
        className={`${styles.VariableQueryEditor.inputForm}`}
        invalid={!selected}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected || null}
        onChange={(change) => onChange(change.value)}
      />
    </>
  );
}
