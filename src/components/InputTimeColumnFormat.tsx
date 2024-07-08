import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { Input } from '@grafana/ui';
import { styles } from '../styles';
import React, { ChangeEvent } from 'react';
import allLabels from '../labels';
import { FormLabel } from './FormLabel';

const DefaultFormat = '1:MILLISECONDS:EPOCH';

export function InputTimeColumnFormat(props: PinotQueryEditorProps) {
  const { query, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeAlias;

  const onChangeFormat = (format: string) => {
    onChange({ ...query, timeColumnFormat: format });
  };

  if (!query.timeColumnFormat) {
    onChangeFormat(DefaultFormat);
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeFormat(event.target.value)}
        value={query.timeColumnFormat}
        width={24}
      />
    </div>
  );
}
