import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel, Input } from '@grafana/ui';
import { styles } from '../styles';
import React, { ChangeEvent } from 'react';

const DefaultFormat = '1:MILLISECONDS:EPOCH';

export function InputTimeColumnFormat(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  const onChangeFormat = (format: string) => {
    onChange({ ...query, timeColumnFormat: format });
  };

  if (!query.timeColumnFormat) {
    onChangeFormat(DefaultFormat);
  }

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Time column format.'}>
        Time Format
      </InlineFormLabel>
      <Input
        className={`width-15 ${styles.Common.inlineSelect}`}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeFormat(event.target.value)}
        value={query.timeColumnFormat}
        width={24}
      />
    </div>
  );
}
