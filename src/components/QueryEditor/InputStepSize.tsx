import React from 'react';
import allLabels from '../../labels';
import { InputTextField } from './InputTextField';

export function InputStepSize(props: { current: string; onChange: (val: string) => void }) {
  return (
    <div className={'gf-form'} data-testid="input-step-size">
      <InputTextField
        current={props.current}
        labels={allLabels.components.QueryEditor.stepSize}
        placeholder={'auto'}
        onChange={props.onChange}
      />
    </div>
  );
}
