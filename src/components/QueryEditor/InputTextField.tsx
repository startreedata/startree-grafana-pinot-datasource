import { FormLabel } from './FormLabel';
import { Input } from '@grafana/ui';
import { styles } from '../../styles';
import React, { ChangeEvent, useEffect, useState } from 'react';

const DefaultDelayMs = 300;

export function InputTextField({
  current,
  delayMs,
  invalid,
  labels: { label, tooltip },
  onChange,
  placeholder,
}: {
  current: string | undefined;
  labels: { label: string; tooltip: string };
  delayMs?: number;
  invalid?: boolean;
  placeholder?: string;
  onChange: (val: string) => void;
}) {
  const [value, setValue] = useState<string | undefined>(current);

  useEffect(() => {
    const timeoutId = setTimeout(
      () => value !== undefined && current !== value && onChange(value),
      delayMs || DefaultDelayMs
    );
    return () => clearTimeout(timeoutId);
  }, [value, current, onChange, delayMs]);

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={tooltip} label={label} />
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => setValue(event.target.value)}
        invalid={invalid}
        placeholder={placeholder}
        value={value}
      />
    </div>
  );
}
