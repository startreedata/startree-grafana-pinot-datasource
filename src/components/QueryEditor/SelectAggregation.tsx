import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React, { useEffect } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';

export const AggregationFunction = Object.freeze({
  COUNT: 'COUNT',
  SUM: 'SUM',
  AVG: 'AVG',
  MAX: 'MAX',
  MIN: 'MIN',
  NONE: 'NONE',
});

const DefaultAggregationFunction = AggregationFunction.SUM;

export function SelectAggregation(props: { selected: string; onChange: (val: string) => void }) {
  const { selected, onChange } = props;
  const labels = allLabels.components.QueryEditor.aggregation;

  useEffect(() => {
    if (!selected && selected !== DefaultAggregationFunction) {
      onChange(DefaultAggregationFunction);
    }
  }, [selected, onChange]);

  return (
    <div className={'gf-form'} data-testid="select-aggregation">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        options={Object.values(AggregationFunction).map((val) => ({ label: val, value: val }))}
        value={selected}
        onChange={(change) => onChange(change.value || '')}
      />
    </div>
  );
}
