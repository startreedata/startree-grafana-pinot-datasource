import React from 'react';
import labels from '../../labels';
import { InputTextField } from './InputTextField';
import { DisplayType } from '../../dataquery/DisplayType';

export function InputMetricLegend(props: { current: string; displayType: string; onChange: (val: string) => void }) {
  if (props.displayType !== DisplayType.TIMESERIES) {
    return <></>;
  }

  return (
    <div className={'gf-form'} data-testid="input-metric-legend">
      <InputTextField
        current={props.current}
        labels={labels.components.QueryEditor.metricLegend}
        onChange={props.onChange}
      />
    </div>
  );
}
