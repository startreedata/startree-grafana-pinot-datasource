import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { MyDataSourceOptions, MyQuery } from '../types';

const queryTypeOptions = [
  { label: 'PinotQL', value: 0 },
  { label: 'PromQL', value: 1},
  { label: 'LogQL', value: 2},
];

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onQueryTextChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, queryText: event.target.value });
  };

  const onTableNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, tableName: event.target.value });
  };


  const onQueryTypeChange = (value: SelectableValue<number>) => {
    onChange({ ...query, queryType: value.label });
  };

  const { queryText, tableName, queryType } = query;

  return (
    <div className="gf-form">
      <InlineField label="Table" labelWidth={16} tooltip="Supply table name">
        <Input onChange={onTableNameChange} value={tableName || ''} width={24}/>
      </InlineField>
      <InlineField label="Query Type" labelWidth={16} tooltip="Select query type">
      <Select options={queryTypeOptions} value={queryTypeOptions.find(option => option.label === queryType) || queryTypeOptions[0]} onChange={onQueryTypeChange}/>
      </InlineField>
      <InlineField label="Query Text" labelWidth={16} tooltip="Query">
        <Input onChange={onQueryTextChange} value={queryText || ''} width={80} />
      </InlineField>
    </div>
  );
}
