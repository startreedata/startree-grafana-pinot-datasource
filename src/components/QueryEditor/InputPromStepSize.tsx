import React from 'react';
import labels from '../../labels';
import { InputTextField } from './InputTextField';

export function InputPromStepSize(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <InputTextField
      current={props.current}
      labels={labels.components.QueryEditor.stepSize}
      onChange={props.onChange}
      data-testid="prom-step-size"
    />
  );
}
