import React from 'react';
import { PinotQueryEditorProps } from '../../dataquery/PinotQueryEditorProps';
import { interpolateVariables } from '../../dataquery/PinotDataQuery';
import { PinotQlTimeSeriesBuilder } from './PinotQlTimeSeriesBuilder';
import { DisplayType } from '../../dataquery/DisplayType';
import { PinotQlLogsBuilder } from './PinotQlLogsBuilder';
import { SelectDisplayType } from './SelectDisplayType';
import { LogsBuilder, TimeSeriesBuilder } from '../../pinotql';

export function PinotQlBuilder(props: PinotQueryEditorProps) {
  return (
    <>
      <SelectDisplayType
        value={props.query.displayType || DisplayType.TIMESERIES}
        displayTypes={[DisplayType.TIMESERIES, DisplayType.LOGS]}
        onChange={(displayType) => {
          props.onChange({ ...props.query, displayType });
          props.onRunQuery();
        }}
      />
      {(() => {
        switch (props.query.displayType) {
          case DisplayType.LOGS:
            return (
              <PinotQlLogsBuilder
                datasource={props.datasource}
                timeRange={{
                  to: props.range?.to,
                  from: props.range?.from,
                }}
                savedParams={LogsBuilder.paramsFrom(props.query)}
                interpolatedParams={LogsBuilder.paramsFrom(
                  interpolateVariables(props.query, props.data?.request?.scopedVars)
                )}
                onChange={(params) => props.onChange(LogsBuilder.dataQueryOf(props.query, params))}
                onRunQuery={props.onRunQuery}
              />
            );
          default:
            return (
              <PinotQlTimeSeriesBuilder
                datasource={props.datasource}
                timeRange={{
                  to: props.range?.to,
                  from: props.range?.from,
                }}
                intervalSize={props.data?.request?.interval}
                savedParams={TimeSeriesBuilder.paramsFrom(props.query)}
                interpolatedParams={TimeSeriesBuilder.paramsFrom(
                  interpolateVariables(props.query, props.data?.request?.scopedVars)
                )}
                onChange={(params) => props.onChange(TimeSeriesBuilder.dataQueryOf(props.query, params))}
                onRunQuery={props.onRunQuery}
              />
            );
        }
      })()}
    </>
  );
}
