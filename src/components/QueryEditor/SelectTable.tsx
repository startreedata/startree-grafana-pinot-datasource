import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTable(props: {
  selected: string;
  options: string[];
  isLoading: boolean;
  onChange: (val: string) => void;
}) {
  const { selected, options, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.table;

  if (options && options?.length === 1 && selected !== options[0]) {
    onChange(options[0]);
  }

  return (
    <div className={'gf-form'} data-testid="select-table">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-table-dropdown">
        <Select
          className={`${styles.QueryEditor.inputForm}`}
          invalid={!selected}
          options={options?.map((name) => ({ label: name, value: name }))}
          value={selected || null}
          isLoading={isLoading}
          onChange={(change) => onChange(change.value || '')}
        />
      </div>
    </div>
  );
}
