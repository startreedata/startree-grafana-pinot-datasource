import { FormLabel } from './FormLabel';
import { Input } from '@grafana/ui';
import { styles } from '../../styles';
import React, { ChangeEvent, useState } from 'react';

export function InputTextField({
  current,
  invalid,
  labels: { label, tooltip },
  onChange,
  placeholder,
}: {
  current: string | undefined;
  labels: { label: string; tooltip: string };
  invalid?: boolean;
  placeholder?: string;
  onChange: (val: string) => void;
}) {
  const [value, setValue] = useState<string | undefined>(current);

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={tooltip} label={label} />
      <Input
        className={`${styles.QueryEditor.inputForm}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => setValue(event.target.value)}
        invalid={invalid}
        placeholder={placeholder}
        value={value}
        onBlur={() => value !== undefined && current !== value && onChange(value)}
      />
    </div>
  );
}
