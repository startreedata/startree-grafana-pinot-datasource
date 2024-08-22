import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export function SelectMetricColumn(props: {
  selected: string | undefined;
  metricColumns: string[];
  isLoading: boolean;
  onChange: (val: string | undefined) => void;
  disabled: boolean;
}) {
  const { disabled, selected, metricColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricColumn;

  if (!selected && metricColumns.length > 0 && selected !== metricColumns[0]) {
    onChange(metricColumns[0]);
  }

  const options = [selected, ...metricColumns]
    .filter((v, i, a) => a.indexOf(v) === i)
    .map((name) => ({ label: name, value: name }))
    .sort();

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        isLoading={isLoading}
        options={options}
        value={selected || null}
        disabled={disabled}
        onChange={(change) => {
          onChange(change.value);
        }}
      />
    </div>
  );
}
