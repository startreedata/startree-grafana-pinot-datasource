import { FormLabel } from './FormLabel';
import { Input } from '@grafana/ui';
import { styles } from '../../styles';
import React, { ChangeEvent } from 'react';

export function InputTextField(props: {
  current: string | undefined;
  labels: { label: string; tooltip: string };
  onChange: (val: string) => void;
}) {
  const { current, labels, onChange } = props;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChange(event.target.value)}
        value={current}
      />
    </div>
  );
}
