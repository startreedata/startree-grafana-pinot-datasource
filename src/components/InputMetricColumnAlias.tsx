import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { Input } from '@grafana/ui';
import React, { ChangeEvent } from 'react';
import { styles } from '../styles';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function InputMetricColumnAlias(props: PinotQueryEditorProps) {
  const { query, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricAlias;

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) =>
          onChange({
            ...query,
            metricColumnAlias: event.target.value,
          })
        }
        placeholder={labels.placeholder}
        value={query.metricColumnAlias}
        width={24}
      />
    </div>
  );
}
