import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { DisplayType } from '../../dataquery/DisplayType';

const DisplayTypeOptions = [
  { label: 'Time Series', value: DisplayType.TIMESERIES },
  { label: 'Table', value: DisplayType.TABLE },
  { label: 'Logs', value: DisplayType.LOGS },
];

export function SelectDisplayType(props: { value: string; displayTypes?: string[]; onChange: (val: string) => void }) {
  const { value, displayTypes, onChange } = props;
  const labels = allLabels.components.QueryEditor.display;

  const options = displayTypes
    ? DisplayTypeOptions.filter(({ value }) => displayTypes.includes(value))
    : DisplayTypeOptions;

  return (
    <div className={'gf-form'} data-testid="select-display-type">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <RadioButtonGroup
        data-testid="radio"
        options={options}
        onChange={onChange}
        value={value || DisplayType.TIMESERIES}
      />
    </div>
  );
}
