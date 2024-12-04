import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { TimeColumn } from '../../resources/controller';

export function SelectTimeColumn(props: {
  selected: string | undefined;
  timeColumns: TimeColumn[];
  isLoading: boolean;
  onChange: (val: string | undefined) => void;
}) {
  const { selected, timeColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeColumn;

  const candidates = timeColumns
    .filter((t) => !t.isDerived)
    // Time columns with derived granularities should come first.
    .sort((a, b) => Number(b.hasDerivedGranularities) - Number(a.hasDerivedGranularities))
    .map((t) => ({
      label: t.name,
      value: t.name,
    }));

  if (!selected && candidates.length > 0 && selected !== candidates[0].value) {
    onChange(candidates[0].value);
  }

  return (
    <div className={'gf-form'} data-testid="select-time-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        isLoading={isLoading}
        options={candidates}
        value={selected || null}
        onChange={(change) => {
          onChange(change.value);
        }}
      />
    </div>
  );
}
