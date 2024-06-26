import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { InlineFormLabel } from '@grafana/ui';
import React from 'react';
import { SQLEditor } from '@grafana/experimental';

export function SqlEditor(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  const onChangeCode = (value: string) => onChange({ ...props.query, pinotQlCode: value });

  if (!query.pinotQlCode) {
    onChangeCode('SELECT * FROM __tableName WHERE __timeFilter;');
  }

  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'row' }}>
      <div>
        <InlineFormLabel width={8} className="query-keyword" tooltip={'Sql Editor'}>
          Pinot Query
        </InlineFormLabel>
      </div>
      <div style={{ flex: '1 1 auto' }}>
        <SQLEditor query={query.pinotQlCode || ''} onChange={onChangeCode} />
      </div>
    </div>
  );
}
