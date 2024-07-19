import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';

export const DisplayTypeTimeSeries = 'TIMESERIES';
export const DisplayTypeTable = 'TABLE';

const DisplayTypes = [
  { label: 'Table', value: DisplayTypeTable },
  { label: 'Time Series', value: DisplayTypeTimeSeries },
];

export function SelectDisplayType(props: { value: string | undefined; onChange: (val: string) => void }) {
  const { value, onChange } = props;
  if (value === undefined) {
    onChange('TABLE');
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={''} label={'Display'} />
      <RadioButtonGroup options={DisplayTypes} onChange={onChange} value={value} />
    </div>
  );
}
