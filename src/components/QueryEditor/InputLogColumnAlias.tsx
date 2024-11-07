import { InputTextField } from './InputTextField';
import labels from '../../labels';
import React from 'react';

export function InputLogColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <InputTextField
      current={props.current}
      placeholder={labels.components.QueryEditor.logAlias.placeholder}
      labels={labels.components.QueryEditor.logAlias}
      onChange={props.onChange}
      data-testid="log-column-alias"
    />
  );
}
