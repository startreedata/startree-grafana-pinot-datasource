import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTimeColumn(props: {
  selected?: string;
  options?: string[] | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeColumn;

  let forced = selected;
  if (options && forced && !options.includes(forced)) {
    forced = undefined;
  }
  if (options && options?.length === 1 && forced === undefined) {
    forced = options[0];
  }
  if (forced !== selected) {
    onChange(forced);
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        invalid={!selected}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </div>
  );
}
