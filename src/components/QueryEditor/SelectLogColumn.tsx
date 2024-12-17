import { columnLabelOf, ComplexField } from '../../types/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';

export function SelectLogColumn(props: {
  selected: ComplexField | undefined;
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField | undefined) => void;
}) {
  const { selected, columns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.logColumn;

  if (!selected && columns.length > 0) {
    onChange({ name: columns[0].name, key: columns[0].key || undefined });
  }

  const selectableColumns = selected ? [selected, ...columns] : columns;
  const options = selectableColumns
    .map(({ name, key }) => columnLabelOf(name, key))
    .filter((v, i, a) => a.indexOf(v) === i)
    .map((label) => ({ label, value: label }));

  return (
    <div className={'gf-form'} data-testid="select-log-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        isLoading={isLoading}
        options={options}
        value={columnLabelOf(selected?.name, selected?.key)}
        onChange={(change) => {
          const col = selectableColumns.find(({ name, key }) => columnLabelOf(name, key) === change.label);
          onChange({ name: col?.name, key: col?.key || undefined });
        }}
      />
    </div>
  );
}
