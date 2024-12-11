import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';
import { columnLabelOf, ComplexField } from '../../types/ComplexField';

export function SelectMetricColumn(props: {
  selected: ComplexField | undefined;
  metricColumns: Column[];
  isCount: boolean;
  isLoading: boolean;
  onChange: (val: ComplexField | undefined) => void;
}) {
  const { isCount, selected, metricColumns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metricColumn;

  // TODO: Does sorting make sense here?
  metricColumns.sort((a, b) => columnLabelOf(a.name, a.key).localeCompare(columnLabelOf(b.name, b.key)));

  if (!isCount && !selected && metricColumns.length > 0) {
    onChange({ name: metricColumns[0].name, key: metricColumns[0].key || undefined });
  }

  const selectableColumns = selected ? [selected, ...metricColumns] : metricColumns;
  const options = selectableColumns
    .map(({ name, key }) => columnLabelOf(name, key))
    .filter((v, i, a) => a.indexOf(v) === i)
    .map((label) => ({ label, value: label }));

  return (
    <div className={'gf-form'} data-testid="select-metric-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        isLoading={isLoading}
        options={isCount ? [{ label: '*', value: '*' }] : options}
        value={isCount ? '*' : columnLabelOf(selected?.name, selected?.key)}
        disabled={isCount}
        onChange={(change) => {
          const col = selectableColumns.find(({ name, key }) => columnLabelOf(name, key) === change.label);
          onChange({ name: col?.name, key: col?.key || undefined });
        }}
      />
    </div>
  );
}
