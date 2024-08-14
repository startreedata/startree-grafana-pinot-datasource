import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTimeColumn(props: {
  selected: string | undefined;
  options: string[];
  isLoading: boolean;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeColumn;

  if (options.length === 1 && selected !== options[0]) {
    onChange(options[0]);
  } else if (selected && !options.includes(selected)) {
    onChange(undefined);
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        invalid={!selected}
        isLoading={isLoading}
        options={options.map((name) => ({ label: name, value: name }))}
        value={selected || null}
        onChange={(change) => {
          onChange(change.value);
        }}
      />
    </div>
  );
}
