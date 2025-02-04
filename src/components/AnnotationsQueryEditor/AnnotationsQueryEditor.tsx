import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { PinotQlCode } from '../QueryEditor/PinotQlCode';
import { CodeQuery } from '../../pinotql';
import { interpolateVariables } from '../../dataquery/PinotDataQuery';
import { DisplayType } from '../../dataquery/DisplayType';

const DefaultAnnotationsQuery =
  //language=text
  `SELECT
  $__timeGroup("timestamp") AS "time",
  'My annotation' as "title",
  'My annotation text' as "text"
FROM $__table()
WHERE $__timeFilter("timestamp")
GROUP BY "time"
LIMIT 100000`;

export function AnnotationsQueryEditor(props: PinotQueryEditorProps) {
  const savedParams:CodeQuery.Params = CodeQuery.paramsFrom(props.query);
  savedParams.displayType = DisplayType.ANNOTATIONS;
  if (!savedParams.pinotQlCode) {
    savedParams.pinotQlCode = DefaultAnnotationsQuery;
  }

  return (
    <PinotQlCode
      datasource={props.datasource}
      query={props.query}
      timeRange={{ to: props.range?.to, from: props.range?.from }}
      intervalSize={props.data?.request?.interval}
      savedParams={savedParams}
      interpolatedParams={CodeQuery.paramsFrom(interpolateVariables(props.query, props.data?.request?.scopedVars))}
      onChange={(params) => props.onChange(CodeQuery.dataQueryOf(props.query, params))}
      onRunQuery={props.onRunQuery}
    />
  );
}
