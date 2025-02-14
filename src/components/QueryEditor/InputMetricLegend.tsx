import React from 'react';
import labels from '../../labels';
import { InputTextField } from './InputTextField';

export function InputMetricLegend(props: { current: string; onChange: (val: string) => void }) {
  return (
    <div className={'gf-form'} data-testid="input-metric-legend">
      <InputTextField
        current={props.current}
        labels={labels.components.QueryEditor.metricLegend}
        onChange={props.onChange}
      />
    </div>
  );
}
