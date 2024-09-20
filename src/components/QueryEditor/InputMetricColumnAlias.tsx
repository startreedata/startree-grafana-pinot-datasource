import React from 'react';
import { InputTextField } from './InputTextField';
import labels from '../../labels';

export function InputMetricColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <InputTextField
      current={props.current}
      labels={labels.components.QueryEditor.metricAlias}
      onChange={props.onChange}
      data-testid="metric-column-alias"
    />
  );
}
