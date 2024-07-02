import { Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectAggregation(props: {
  selected: string | undefined;
  options: string[];
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={'Select aggregation function'} label={'Aggregation'} />
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={options.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </div>
  );
}
