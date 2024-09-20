import React from 'react';
import labels from '../../labels';
import { InputTextField } from './InputTextField';

export function InputTimeColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  return (
    <InputTextField
      placeholder={'time'}
      current={props.current}
      labels={labels.components.QueryEditor.timeAlias}
      onChange={props.onChange}
    />
  );
}
