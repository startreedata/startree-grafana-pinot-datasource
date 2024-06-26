import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { useTableSchema } from '../resources/resources';
import { InlineFormLabel, MultiSelect } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';

export function SelectGroupBy(props: PinotQueryEditorProps) {
  const { datasource, query, onChange, onRunQuery } = props;

  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const dimensionColumns = schema?.dimensionFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select dimensions function'}>
        Group By
      </InlineFormLabel>
      <MultiSelect
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(dimensionColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.dimensionColumns}
        onChange={(item: SelectableValue<string>[]) => {
          const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
          onChange({ ...query, dimensionColumns: selected });
          onRunQuery();
        }}
      />
    </div>
  );
}
