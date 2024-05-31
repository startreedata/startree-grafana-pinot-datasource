import React, { ChangeEvent } from 'react';
import { Input } from '@grafana/ui';
import { styles } from '../../styles';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

const DefaultTimeColumnAlias = 'time';

export function InputTimeColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeAlias;

  if (current === undefined) {
    onChange(DefaultTimeColumnAlias);
  }

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
