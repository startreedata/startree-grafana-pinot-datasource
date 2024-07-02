import { Select } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectDatabase(props: {
  options: string[] | undefined;
  selected: string | undefined;
  defaultValue: string;
  onChange: (val: string | undefined) => void;
}) {
  const { defaultValue, selected, options, onChange } = props;

  if (options?.length == 0 && selected == undefined) {
    onChange(defaultValue);
  } else if (options?.length == 1 && selected == undefined) {
    onChange(options[0]);
  }

  if (options?.length == 0) {
    options.push(defaultValue);
  }

  return (
    <>
      <FormLabel tooltip={'Select Pinot database'} label={'Database'} />
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        // TODO: Handle the default db name correctly.
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        disabled={[0, 1].includes(options?.length || -1)}
        onChange={(value: SelectableValue<string>) => value.value && onChange(value.value)}
      />
    </>
  );
}
