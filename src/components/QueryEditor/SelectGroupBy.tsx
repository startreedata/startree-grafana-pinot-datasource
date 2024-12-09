import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';
import React, { useEffect } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { ComplexField, getColumnLabel } from '../../types/ComplexField';

export function SelectGroupBy(props: {
  selected: ComplexField[] | undefined;
  columns: ComplexField[];
  isLoading: boolean;
  disabled: boolean;
  onChange: (val: ComplexField[] | undefined) => void;
}) {
  const { columns, selected, disabled, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;

  const labelToColumnMap = columns
    .filter((col) => col.name)
    .reduce((a, b) => a.set(getColumnLabel(b.name, b.key), b), new Map<string, ComplexField>());
  const getColumn = (label: string | undefined) => labelToColumnMap.get(label || '') || { name: '', key: '' };

  useEffect(() => {
    const valid =
      selected?.filter((col: ComplexField) => labelToColumnMap.has(getColumnLabel(col.name, col.key))) || [];
    if (valid.length < (selected?.length || 0)) {
      onChange(valid);
    }
  });

  return (
    <div className={'gf-form'} data-testid="select-group-by">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <MultiSelect
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        options={columns.map((col) => ({ label: getColumnLabel(col.name, col.key) }))}
        value={selected}
        disabled={disabled}
        isLoading={isLoading}
        onChange={(item: Array<SelectableValue<string>>) => {
          const selected = item.map((v) => getColumn(v.label)).filter((col) => col.name);
          onChange(selected);
        }}
      />
    </div>
  );
}
