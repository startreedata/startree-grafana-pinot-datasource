import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { useTableSchema } from '../resources/resources';
import { InlineFormLabel, Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';
import { canRunQuery } from '../types/PinotDataQuery';

export function SelectMetricColumn(props: PinotQueryEditorProps) {
  const { datasource, query, onChange, onRunQuery } = props;

  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const metricColumns = schema?.metricFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select metric column'}>
        Metric Column
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(metricColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.metricColumn}
        onChange={(value) => {
          const newQuery = { ...query, metricColumn: value.value }
          onChange(newQuery);
          if (canRunQuery(newQuery)) {
            onRunQuery();
          }
        }}
      />
    </div>
  );
}
