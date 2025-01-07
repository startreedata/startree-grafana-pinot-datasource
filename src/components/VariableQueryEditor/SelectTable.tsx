import allLabels from '../../labels';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectTable(props: {
  selected: string;
  options: string[];
  isLoading: boolean;
  onChange: (val: string) => void;
}) {
  const { selected, options, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.table;

  return (
    <div className={'gf-form'} data-testid="select-table">
      <FormLabel label={labels.label} tooltip={labels.tooltip} />
      <div data-testid={'select-table-dropdown'}>
        <Select
          className={`${styles.VariableQueryEditor.inputForm}`}
          invalid={!selected}
          isLoading={isLoading}
          options={options?.map((name) => ({ label: name, value: name }))}
          value={selected || null}
          onChange={(change) => onChange(change.value || '')}
        />
      </div>
    </div>
  );
}
