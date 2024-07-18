import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';

const DisplayTypes = [
  { label: 'Time Series', value: 'TIMESERIES' },
  { label: 'Table', value: 'TABLE' },
];

export function SelectDisplayType(props: { value: string | undefined; onChange: (val: string) => void }) {
  const { value, onChange } = props;
  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={''} label={'Display'} />
      <RadioButtonGroup options={DisplayTypes} onChange={onChange} value={value} />
    </div>
  );
}
