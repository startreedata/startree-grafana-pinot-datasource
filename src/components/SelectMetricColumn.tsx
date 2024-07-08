import { Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function SelectMetricColumn(props: {
  selected: string | undefined;
  options: string[] | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, options, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricColumn;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        options={options?.map((name) => ({ label: name, value: name }))}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </div>
  );
}
