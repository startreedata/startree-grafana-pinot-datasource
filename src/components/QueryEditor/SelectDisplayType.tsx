import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export const DisplayTypeTimeSeries = 'TIMESERIES';
export const DisplayTypeTable = 'TABLE';
export const DisplayTypeLogs = 'LOGS';

const DisplayTypes = [
  { label: 'Time Series', value: DisplayTypeTimeSeries },
  { label: 'Table', value: DisplayTypeTable },
  { label: 'Logs', value: DisplayTypeLogs },
];

export function SelectDisplayType(props: { value: string | undefined; onChange: (val: string) => void }) {
  const { value, onChange } = props;
  const labels = allLabels.components.QueryEditor.display;

  return (
    <div className={'gf-form'} data-testid="select-display-type">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <RadioButtonGroup
        data-testid="radio"
        options={DisplayTypes}
        onChange={onChange}
        value={value || DisplayTypeTimeSeries}
      />
    </div>
  );
}
