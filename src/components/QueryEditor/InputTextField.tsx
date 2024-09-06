import { FormLabel } from './FormLabel';
import { Input } from '@grafana/ui';
import { styles } from '../../styles';
import React, { ChangeEvent, useEffect, useState } from 'react';

const DefaultDelayMs = 300;

export function InputTextField(props: {
  current: string | undefined;
  labels: { label: string; tooltip: string };
  delayMs?: number;
  invalid?: boolean;
  placeholder?: string;
  onChange: (val: string) => void;
}) {
  const [value, setValue] = useState<string|undefined>(props.current);

  useEffect(() => {
    const timeoutId = setTimeout(
      () => value && props.current !== value && props.onChange(value),
      props.delayMs || DefaultDelayMs
    );
    return () => clearTimeout(timeoutId);
  }, [value, props.current, props.onChange]);

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={props.labels.tooltip} label={props.labels.label} />
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => setValue(event.target.value)}
        invalid={props.invalid}
        placeholder={props.placeholder}
        value={value}
      />
    </div>
  );
}
