import { columnLabelOf, ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';

export function SelectLogMessageColumn(props: {
  selected: ComplexField;
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField) => void;
}) {
  const { selected, columns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.logColumn;

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
        value={columnLabelOf(selected?.name, selected?.key) || null}
        onChange={(change) => {
          const col = selectableColumns.find(({ name, key }) => columnLabelOf(name, key) === change.label);
          onChange({ name: col?.name, key: col?.key || undefined });
        }}
      />
    </div>
  );
}
