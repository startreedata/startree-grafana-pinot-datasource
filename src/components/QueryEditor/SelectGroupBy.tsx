import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectGroupBy(props: {
  selected: string[] | undefined;
  options: string[];
  isLoading: boolean;
  disabled: boolean;
  onChange: (val: string[] | undefined) => void;
}) {
  const { options, selected, disabled, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;

  if (selected && selected.filter((val) => options.includes(val)).length !== selected.length) {
    onChange(selected.filter((val) => options.includes(val)));
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <MultiSelect
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        options={options.map((name) => ({ label: name, value: name }))}
        value={selected}
        disabled={disabled}
        isLoading={isLoading}
        onChange={(item: Array<SelectableValue<string>>) => {
          const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
          onChange(selected);
        }}
      />
    </div>
  );
}
