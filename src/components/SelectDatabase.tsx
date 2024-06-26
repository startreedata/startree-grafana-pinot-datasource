import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { useDatabases } from '../resources/resources';
import { InlineFormLabel, Select } from '@grafana/ui';
import { styles } from '../styles';
import { SelectableValue } from '@grafana/data';
import React from 'react';
import {GetDefaultPinotDataQuery} from "../types/PinotDataQuery";

export function SelectDatabase(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;
  const databases = useDatabases(datasource);
  const defaultDatabase = GetDefaultPinotDataQuery().databaseName;

  const onChangeDatabase = (value: string | undefined) =>
    onChange({
      ...query,
      databaseName: value,
      tableName: undefined,
      timeColumn: undefined,
      metricColumn: undefined,
      aggregationFunction: undefined,
      dimensionColumns: undefined,
    });

  if (databases.length == 1 && query.databaseName == undefined) {
    onChangeDatabase(databases[0]);
  }

  // TODO: Use AsyncSelect
  return (
    <>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select Pinot database'}>
        Database
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        // TODO: Handle the default db name correctly.
        options={[defaultDatabase, ...databases].map((name) => ({ label: name, value: name }))}
        value={query.databaseName || defaultDatabase}
        disabled={[0, 1].includes(databases.length)}
        onChange={(value: SelectableValue<string>) => onChangeDatabase(value.value)}
      />
    </>
  );
}
