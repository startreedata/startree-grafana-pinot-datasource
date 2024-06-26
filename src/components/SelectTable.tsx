import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { useTables } from '../resources/resources';
import { InlineFormLabel, Select } from '@grafana/ui';
import { styles } from '../styles';
import React from 'react';

export function SelectTable(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  const tables = useTables(datasource, query.databaseName);

  // TODO: Use AsyncSelect
  return (
    <>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select Pinot table'}>
        Table
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={tables.map((name) => ({ label: name, value: name }))}
        value={query.tableName}
        onChange={(value) =>
          onChange({
            ...query,
            tableName: value.value,
            timeColumn: undefined,
            metricColumn: undefined,
            dimensionColumns: undefined,
          })
        }
      />
    </>
  );
}
