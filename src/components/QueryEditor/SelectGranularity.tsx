import React from 'react';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';

const options = [
  { label: 'auto', value: 'auto' },
  { label: 'DAYS', value: 'DAYS' },
  { label: 'HOURS', value: 'HOURS' },
  {
    label: 'MINUTES',
    value: 'MINUTES',
  },
  { label: 'SECONDS', value: 'SECONDS' },
  { label: 'MILLISECONDS', value: 'MILLISECONDS' },
  {
    label: 'MICROSECONDS',
    value: 'MICROSECONDS',
  },
  { label: 'NANOSECONDS', value: 'NANOSECONDS' },
];

export function SelectGranularity(props: {
  selected: string | undefined;
  disabled: boolean;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, disabled, onChange } = props;
  const labels = allLabels.components.QueryEditor.granularity;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        placeholder={'auto'}
        options={options}
        value={selected}
        disabled={disabled}
        onChange={(change) => (change.value !== 'auto' ? onChange(change.value) : onChange(undefined))}
      />
    </div>
  );
}
