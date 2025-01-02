import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';

export function SelectTimeColumn(props: {
  selected: string;
  timeColumns: Column[];
  isLoading: boolean;
  onChange: (val: string) => void;
}) {
  const { selected, timeColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.timeColumn;

  const candidates = timeColumns
    .filter((t) => !t.isDerived)
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
      <div data-testid="select-time-column-dropdown">
        <Select
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          invalid={!selected}
          isLoading={isLoading}
          options={candidates}
          value={selected}
          onChange={(change) => {
            onChange(change.value || '');
          }}
        />
      </div>
    </div>
  );
}
