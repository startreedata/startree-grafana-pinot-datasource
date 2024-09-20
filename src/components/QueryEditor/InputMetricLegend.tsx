import React from 'react';
import labels from '../../labels';
import { InputTextField } from './InputTextField';

export function InputMetricLegend(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <InputTextField
      current={props.current}
      labels={labels.components.QueryEditor.metricLegend}
      onChange={props.onChange}
      data-testid="metric-legend"
    />
  );
}
