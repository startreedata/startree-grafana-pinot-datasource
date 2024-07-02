import { Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectTable(props: {
  selected: string | undefined;
  options: string[] | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;

  return (
    <>
      <FormLabel tooltip={'Select Pinot Table'} label={'Table'} />
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </>
  );
}
