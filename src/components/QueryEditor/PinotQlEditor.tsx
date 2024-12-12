import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { EditorMode } from '../../dataquery/EditorMode';
import { PinotQlBuilder } from './PinotQlBuilder';
import { PinotQlCode } from './PinotQlCode';
import { builderParamsFrom as builderParamsFrom, dataQueryWithBuilderParams } from '../../pinotql/builderParams';
import { interpolateVariables } from '../../dataquery/PinotDataQuery';
import { codeParamsFrom, dataQueryWithCodeParams } from '../../pinotql/codeParams';

export function PinotQlEditor(props: PinotQueryEditorProps) {
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
          interpolatedParams={codeParamsFrom(interpolateVariables(props.query, props.data?.request?.scopedVars))}
          onChange={(params) => props.onChange(dataQueryWithCodeParams(props.query, params))}
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
          interpolatedParams={builderParamsFrom(interpolateVariables(props.query, props.data?.request?.scopedVars))}
          onChange={(params) => props.onChange(dataQueryWithBuilderParams(props.query, params))}
          onRunQuery={props.onRunQuery}
        />
      )}
    </div>
  );
}
