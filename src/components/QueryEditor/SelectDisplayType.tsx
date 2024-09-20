import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export const DisplayTypeTimeSeries = 'TIMESERIES';
export const DisplayTypeTable = 'TABLE';

const DisplayTypes = [
  { label: 'Table', value: DisplayTypeTable },
  { label: 'Time Series', value: DisplayTypeTimeSeries },
];

export function SelectDisplayType(props: { value: string | undefined; onChange: (val: string) => void }) {
  const { value, onChange } = props;
  const labels = allLabels.components.QueryEditor.display;

  return (
    <div className={'gf-form'} data-testid="select-display-type">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <RadioButtonGroup options={DisplayTypes} onChange={onChange} value={value} />
    </div>
  );
}
