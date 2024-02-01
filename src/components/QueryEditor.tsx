import React, { ChangeEvent } from 'react';
import { InlineField, Input } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onQueryTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, queryText: event.target.value });
  };

  const onTableNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, tableName: event.target.value });
    // executes the query
    onRunQuery();
  };

  const { queryText, tableName } = query;

  return (
    <div className="gf-form">
      <InlineField label="Table" labelWidth={24} tooltip="Not used yet">
        <Input onChange={onTableNameChange} value={tableName || ''} width={20}/>
      </InlineField>
      <InlineField label="Query Text" labelWidth={80} tooltip="Not used yet">
        <Input onChange={onQueryTextChange} value={queryText || ''} />
      </InlineField>
    </div>
  );
}
