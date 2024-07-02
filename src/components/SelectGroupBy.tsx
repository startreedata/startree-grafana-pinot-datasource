import { MultiSelect } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import { FormLabel } from './FormLabel';

export function SelectGroupBy(props: {
  selected: string[] | undefined;
  options: string[] | undefined;
  onChange: (val: string[] | undefined) => void;
}) {
  const { options, selected, onChange } = props;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={'Select dimensions function'} label={'Group By'} />
      <MultiSelect
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(item: SelectableValue<string>[]) => {
          const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
          onChange(selected);
        }}
      />
    </div>
  );
}
