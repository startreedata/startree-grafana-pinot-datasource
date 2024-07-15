import { Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

const AggregationOptions = [
  { label: 'SUM', value: 'SUM' },
  { label: 'COUNT', value: 'COUNT' },
  { label: 'AVG', value: 'AVG' },
  { label: 'MAX', value: 'MAX' },
  { label: 'MIN', value: 'MIN' },
  { label: 'NONE', value: 'NONE' },
];

export function SelectAggregation(props: {
  selected: string | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, onChange } = props;
  const labels = allLabels.components.QueryEditor.aggregation;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        options={AggregationOptions}
        value={selected}
        onChange={(change) => onChange(change.value)}
      />
    </div>
  );
}
