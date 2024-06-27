import React, { ChangeEvent } from 'react';
import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, Input } from '@grafana/ui';
import { styles } from '../styles';

const DefaultAlias = 'time';

export function InputTimeColumnAlias(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

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
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Time column alias.'}>
        Time Alias
      </InlineFormLabel>
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeAlias(event.target.value)}
        value={query.timeColumnAlias}
        width={24}
      />
    </div>
  );
}
