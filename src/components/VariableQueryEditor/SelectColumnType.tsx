import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';

export const ColumnTypes = Object.freeze({
  Dimension: 'DIMENSION',
  Metric: 'METRIC',
  DateTime: 'DATETIME',
  All: 'ALL',
});

export function SelectColumnType({
  onChange,
  selected,
}: {
  selected: string | undefined;
  onChange: (val: string | undefined) => void;
}) {
  const labels = allLabels.components.VariableQueryEditor.columnType;

  return (
    <>
      <FormLabel label={labels.label} tooltip={labels.tooltip} />
      <Select
        className={`${styles.VariableQueryEditor.inputForm}`}
        invalid={!selected}
        options={Object.values(ColumnTypes)?.map((name) => ({ label: name, value: name }))}
        value={selected || null}
        onChange={(change) => onChange(change.value)}
      />
    </>
  );
}
