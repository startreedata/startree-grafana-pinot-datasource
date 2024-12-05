import React from 'react';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import { Granularity } from '../../resources/granularities';

export function SelectGranularity(props: {
  selected: string | undefined;
  options: Granularity[];
  disabled: boolean;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, disabled, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.granularity;

  return (
    <div className={'gf-form'} data-testid="select-granularity">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        placeholder={'auto'}
        options={options.map((g) => ({ label: `${g.name}${g.optimized ? '*' : ''}`, value: g.name }))}
        value={selected || null}
        disabled={disabled}
        onChange={(change) => (change.value !== 'auto' ? onChange(change.value) : onChange(undefined))}
      />
    </div>
  );
}
