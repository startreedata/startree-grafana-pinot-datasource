import React from 'react';
import { SelectTable } from './SelectTable';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { usePromQlTables } from '../../resources/controller';
import { FormLabel } from './FormLabel';
import { TextArea } from '@grafana/ui';

export function PromQlEditor(props: PinotQueryEditorProps) {
  const tables = usePromQlTables(props.datasource);

  return (
    <>
      <div className={'gf-form'}>
        <SelectTable
          selected={props.query.tableName}
          options={tables}
          onChange={(tableName) => props.onChange({ ...props.query, tableName })}
        />
      </div>
      <div className={'gf-form'}>
        <>
          <FormLabel tooltip={'Query'} label={'Query'} />
          <TextArea onChange={(event) => props.onChange({ ...props.query, promQlCode: event.currentTarget.value })} />
        </>
      </div>
    </>
  );
}
