import { MultiSelect } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function SelectGroupBy(props: {
  selected: string[] | undefined;
  options: string[] | undefined;
  onChange: (val: string[] | undefined) => void;
}) {
  const { options, selected, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <MultiSelect
        className={`${styles.QueryEditor.inputForm}`}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(item: Array<SelectableValue<string>>) => {
          const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
          onChange(selected);
        }}
      />
    </div>
  );
}
