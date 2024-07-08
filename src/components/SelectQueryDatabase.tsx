import { Select } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function SelectQueryDatabase(props: {
  options: string[] | undefined;
  selected: string | undefined;
  defaultValue: string;
  onChange: (val: string | undefined) => void;
}) {
  const { defaultValue, selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.database;

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
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        // TODO: Handle the default db name correctly.
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        disabled={[0, 1].includes(options?.length || -1)}
        onChange={(value: SelectableValue<string>) => value.value && onChange(value.value)}
      />
    </>
  );
}
