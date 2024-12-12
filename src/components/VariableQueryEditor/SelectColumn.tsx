import allLabels from '../../labels';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectColumn(props: {
  selected: string;
  options: string[];
  isLoading: boolean;
  onChange: (val: string) => void;
}) {
  const { selected, options, isLoading, onChange } = props;
  const labels = allLabels.components.VariableQueryEditor.column;

  return (
    <div className={'gf-form'} data-testid="select-column">
      <FormLabel label={labels.label} tooltip={labels.tooltip} />
      <Select
        className={`${styles.VariableQueryEditor.inputForm}`}
        invalid={!selected}
        options={options.sort().map((name) => ({ label: name, value: name }))}
        isLoading={isLoading}
        value={selected || null}
        onChange={(change) => onChange(change.value || '')}
      />
    </div>
  );
}
