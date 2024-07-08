import { Input } from '@grafana/ui';
import React, { ChangeEvent } from 'react';
import { styles } from '../styles';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function InputMetricColumnAlias(props: { current: string | undefined; onChange: (val: string) => void }) {
  const { current, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricAlias;

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
