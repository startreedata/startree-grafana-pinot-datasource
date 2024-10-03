import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { EditorMode } from '../../types/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';
import { useTables } from '../../resources/controller';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  const tables = useTables(props.datasource);

  return (
    <div>
      {props.query.editorMode === EditorMode.Code ? (
        <PinotQlCode
          datasource={props.datasource}
          query={props.query}
          timeRange={{
            to: props.range?.to,
            from: props.range?.from,
          }}
          intervalSize={props.data?.request?.interval}
          tables={tables}
          scopedVars={props.data?.request?.scopedVars || {}}
          onChange={props.onChange}
          onRunQuery={props.onRunQuery}
        />
      ) : (
        <PinotQlBuilder
          datasource={props.datasource}
          query={props.query}
          timeRange={{
            to: props.range?.to,
            from: props.range?.from,
          }}
          intervalSize={props.data?.request?.interval}
          tables={tables}
          scopedVars={props.data?.request?.scopedVars || {}}
          onChange={props.onChange}
          onRunQuery={props.onRunQuery}
        />
      )}
    </div>
  );
}
