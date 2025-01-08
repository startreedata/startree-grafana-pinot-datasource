import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { EditorMode } from '../../dataquery/EditorMode';
import { PinotQlCode } from './PinotQlCode';
import { interpolateVariables } from '../../dataquery/PinotDataQuery';
import { PinotQlBuilder } from './PinotQlBuilder';
import { CodeQuery } from '../../pinotql/CodeQuery';

export function PinotQlEditor(props: PinotQueryEditorProps) {
  switch (props.query.editorMode) {
    case EditorMode.Code:
      return (
        <PinotQlCode
          datasource={props.datasource}
          query={props.query}
          timeRange={{
            to: props.range?.to,
            from: props.range?.from,
          }}
          intervalSize={props.data?.request?.interval}
          savedParams={CodeQuery.paramsFrom(props.query)}
          interpolatedParams={CodeQuery.paramsFrom(interpolateVariables(props.query, props.data?.request?.scopedVars))}
          onChange={(params) => props.onChange(CodeQuery.dataQueryOf(props.query, params))}
          onRunQuery={props.onRunQuery}
        />
      );
    default:
      return <PinotQlBuilder {...props} />;
  }
}
