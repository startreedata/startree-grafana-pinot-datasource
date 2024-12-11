import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { EditorMode } from '../../types/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';
import { useTables } from '../../resources/controller';
import { dataQueryWithBuilderParams, builderParamsFrom as builderParamsFrom } from '../../pinotql/builderParams';
import { interpolateVariables } from '../../types/PinotDataQuery';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  const tables = useTables(props.datasource);
  const scopedVars = props.data?.request?.scopedVars || {};

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
          tables={tables.result}
          scopedVars={scopedVars}
          onChange={props.onChange}
          onRunQuery={props.onRunQuery}
        />
      ) : (
        <PinotQlBuilder
          datasource={props.datasource}
          timeRange={{
            to: props.range?.to,
            from: props.range?.from,
          }}
          intervalSize={props.data?.request?.interval}
          savedParams={builderParamsFrom(props.query)}
          interpolatedParams={builderParamsFrom(interpolateVariables(props.query, scopedVars))}
          onChange={(params) => dataQueryWithBuilderParams(props.query, params)}
          onRunQuery={props.onRunQuery}
        />
      )}
    </div>
  );
}
