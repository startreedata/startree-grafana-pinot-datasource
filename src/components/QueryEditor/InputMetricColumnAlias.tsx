import React from 'react';
import { InputTextField } from './InputTextField';
import labels from '../../labels';

export function InputMetricColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <div className={'gf-form'} data-testid="input-metric-alias">
      <InputTextField
        current={props.current}
        placeholder={'metric'}
        labels={labels.components.QueryEditor.metricAlias}
        onChange={props.onChange}
      />
    </div>
  );
}
