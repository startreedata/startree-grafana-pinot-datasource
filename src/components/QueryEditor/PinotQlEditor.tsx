import React from 'react';
import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { EditorMode } from '../../types/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';
import { builderParamsFrom as builderParamsFrom, dataQueryWithBuilderParams } from '../../pinotql/builderParams';
import { interpolateVariables } from '../../types/PinotDataQuery';
import { codeParamsFrom, dataQueryWithCodeParams } from '../../pinotql/codeParams';

export function PinotQlEditor(props: PinotQueryEditorProps) {
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
          savedParams={codeParamsFrom(props.query)}
          interpolatedParams={codeParamsFrom(interpolateVariables(props.query, scopedVars))}
          onChange={(params) => dataQueryWithCodeParams(props.query, params)}
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
