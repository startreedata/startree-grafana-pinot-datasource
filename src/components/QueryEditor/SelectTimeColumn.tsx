import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectTimeColumn(props: {
  selected: string | undefined;
  timeColumns: string[];
  isLoading: boolean;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, timeColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeColumn;

  if (!selected && timeColumns.length > 0 && selected !== timeColumns[0]) {
    onChange(timeColumns[0]);
  }

  const options = [selected, ...timeColumns]
    .filter((v, i, a) => a.indexOf(v) === i)
    .map((name) => ({ label: name, value: name }))
    .sort();

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        invalid={!selected}
        isLoading={isLoading}
        options={options}
        value={selected || null}
        onChange={(change) => {
          onChange(change.value);
        }}
      />
    </div>
  );
}
