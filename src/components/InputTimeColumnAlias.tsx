import React, { ChangeEvent } from 'react';
import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { Input } from '@grafana/ui';
import { styles } from '../styles';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

const DefaultAlias = 'time';

export function InputTimeColumnAlias(props: PinotQueryEditorProps) {
  const { query, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeAlias;

  const onChangeAlias = (alias: string) =>
    onChange({
      ...query,
      timeColumnAlias: alias,
    });

  if (!query.timeColumnAlias) {
    onChangeAlias(DefaultAlias);
  }

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeAlias(event.target.value)}
        value={query.timeColumnAlias}
        width={24}
      />
    </div>
  );
}
