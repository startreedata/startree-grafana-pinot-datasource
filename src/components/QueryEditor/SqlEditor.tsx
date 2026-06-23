import React, { useMemo, useRef, useState } from 'react';
import { SQLEditor as GrafanaSqlEditor } from '@grafana/experimental';
import { Button } from '@grafana/ui';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { DataSource } from '../../datasource';
import { listTables } from '../../resources/tables';
import { listColumns } from '../../resources/columns';
import { columnLabelOf } from '../../pinotql/complexField';
import { buildPinotLanguageDefinition } from '../../pinotql/pinotLanguageDefinition';

export function SqlEditor(props: {
  current: string;
  onChange: (val: string) => void;
  datasource?: DataSource;
  tableName?: string;
}) {
  const { current, onChange, datasource, tableName } = props;
  const labels = allLabels.components.QueryEditor.sqlEditor;

  const [editorContent, setEditorContent] = useState(current);

  // Refs so the completion resolvers always read the latest datasource/table without rebuilding
  // (and re-registering) the Monaco language whenever the selected table changes.
  const datasourceRef = useRef(datasource);
  datasourceRef.current = datasource;
  const tableNameRef = useRef(tableName);
  tableNameRef.current = tableName;

  const language = useMemo(
    () =>
      buildPinotLanguageDefinition({
        resolveTables: () => (datasourceRef.current ? listTables(datasourceRef.current) : Promise.resolve([])),
        resolveColumns: async () => {
          const ds = datasourceRef.current;
          const table = tableNameRef.current;
          if (!ds || !table) {
            return [];
          }
          const columns = await listColumns(ds, { tableName: table });
          return columns.map((column) => ({ name: columnLabelOf(column.name, column.key), type: column.dataType }));
        },
      }),
    []
  );

  return (
    <div className={'gf-form'} data-testid="sql-editor-container">
      <div>
        <FormLabel tooltip={labels.tooltip} label={labels.label} />
      </div>
      <div style={{ flex: '1 1 auto' }} data-testid="sql-editor-content">
        <GrafanaSqlEditor
          query={editorContent}
          onChange={setEditorContent}
          onBlur={() => current !== editorContent && onChange(editorContent)}
          language={language}
        >
          {({ formatQuery }) => (
            <div style={{ marginTop: '4px' }}>
              <Button size="sm" variant="secondary" icon="brackets-curly" onClick={formatQuery}>
                {labels.format}
              </Button>
            </div>
          )}
        </GrafanaSqlEditor>
      </div>
    </div>
  );
}
