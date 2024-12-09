import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';
import React, { useEffect } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { Column } from '../../resources/columns';

export function SelectGroupBy(props: {
  selected: Column[] | undefined;
  columns: Column[];
  isLoading: boolean;
  disabled: boolean;
  onChange: (val: Column[] | undefined) => void;
}) {
  const { columns, selected, disabled, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;

  const getColumnLabel = (col: Column) => (col.key ? `${col.name}['${col.key}']` : col.name);
  const labelToColumnMap = columns
    .filter((col) => col.name)
    .reduce((a, b) => a.set(getColumnLabel(b), b), new Map<string, Column>());
  const getColumn = (label: string | undefined) => labelToColumnMap.get(label || '') || { name: '', key: '' };

  useEffect(() => {
    const valid = selected?.filter((col: Column) => labelToColumnMap.has(getColumnLabel(col))) || [];
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
        options={columns.map((col) => ({ label: getColumnLabel(col) }))}
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
