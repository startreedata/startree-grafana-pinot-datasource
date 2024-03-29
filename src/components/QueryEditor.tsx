import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select, InlineFieldRow } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { PinotConnectionConfig } from '../types/config';
import {  PinotQuery } from '../types/sql';

const queryTypeOptions = [
  { label: 'PinotQL', value: 0 },
  { label: 'PromQL', value: 1},
  { label: 'LogQL', value: 2},
];

type Props = QueryEditorProps<DataSource, PinotQuery, PinotConnectionConfig>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onRawSqlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, rawSql: event.target.value });
  };

  const onTableNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, tableName: event.target.value });
  };

  const onQueryTypeChange = (value: SelectableValue<number>) => {
    onChange({ ...query, queryType: value.label });
  };

  const { rawSql, tableName, queryType } = query;

  return (
    <div className="gf-form">
      <InlineFieldRow>
        <InlineField label="Table" labelWidth={16} tooltip="Supply table name">
          <Input onChange={onTableNameChange} value={tableName || ''} width={24}/>
        </InlineField>
        <InlineField label="Query Type" labelWidth={16} tooltip="Query Type">
        <Select options={queryTypeOptions} value={queryTypeOptions.find(option => option.label === queryType) || queryTypeOptions[0]} onChange={onQueryTypeChange}/>
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
      <InlineField label="Raw Query" labelWidth={16} tooltip="Raw Query SQL">
        <Input onChange={onRawSqlChange} value={rawSql || ''} width={700} />
      </InlineField>
      </InlineFieldRow>
    </div>
  );
}
