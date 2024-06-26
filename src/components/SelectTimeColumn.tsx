import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { useTableSchema } from '../resources/resources';
import { InlineFormLabel, Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';

export function SelectTimeColumn(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  // TODO: Pass this as a param
  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const timeColumns = schema?.dateTimeFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select time column'}>
        Time Column
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(timeColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.timeColumn}
        onChange={(value) => onChange({ ...query, timeColumn: value.value })}
      />
    </div>
  );
}
