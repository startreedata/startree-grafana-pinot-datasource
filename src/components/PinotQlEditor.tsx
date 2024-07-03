import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectQueryDatabase } from './SelectQueryDatabase';
import { SelectTable } from './SelectTable';
import { EditorMode } from '../types/EditorMode';
import React from 'react';
import { PinotQlBuilderEditor } from './PinotQlBuilderEditor';
import { PinotQlCodeEditor } from './PinotQlCodeEditor';
import { useDatabases, useTables } from '../resources/resources';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;
  const databases = useDatabases(datasource);
  const tables = useTables(datasource, query.databaseName);
  const defaultDatabase = 'default';

  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'}>
          <SelectQueryDatabase
            options={databases}
            selected={query.databaseName}
            defaultValue={defaultDatabase}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                databaseName: value,
                tableName: undefined,
                timeColumn: undefined,
                metricColumn: undefined,
                groupByColumns: undefined,
                aggregationFunction: undefined,
                filters: undefined,
              })
            }
          />
          <SelectTable
            options={tables}
            selected={query.tableName}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                tableName: value,
                timeColumn: undefined,
                metricColumn: undefined,
                groupByColumns: undefined,
                aggregationFunction: undefined,
                filters: undefined,
              })
            }
          />
        </div>
      </div>
      {props.query.editorMode == EditorMode.Code ? (
        <PinotQlCodeEditor {...props} />
      ) : (
        <PinotQlBuilderEditor {...props} />
      )}
    </div>
  );
}
