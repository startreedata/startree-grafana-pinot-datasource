import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { SelectDatabase } from './SelectDatabase';
import { SelectTable } from './SelectTable';
import { EditorMode } from '../../types/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';
import { useDatabases, useTables } from '../../resources/controller';

const DefaultDatabase = 'default';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;
  const databases = useDatabases(datasource);
  const tables = useTables(datasource, query.databaseName);

  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'}>
          <SelectDatabase
            options={databases}
            selected={query.databaseName}
            defaultValue={DefaultDatabase}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                databaseName: value,
                tableName: undefined,
                timeColumn: undefined,
                metricColumn: undefined,
                groupByColumns: undefined,
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
      {props.query.editorMode === EditorMode.Code ? <PinotQlCode {...props} /> : <PinotQlBuilder {...props} />}
    </div>
  );
}
