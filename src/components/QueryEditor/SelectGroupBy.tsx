import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';
import React, { useEffect } from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { columnLabelOf, ComplexField } from '../../types/ComplexField';
import { Column } from '../../resources/columns';

export function SelectGroupBy(props: {
  selected: ComplexField[] | undefined;
  columns: Column[];
  isLoading: boolean;
  disabled: boolean;
  onChange: (val: ComplexField[] | undefined) => void;
}) {
  const { columns, selected, disabled, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;

  const selectOptions =
    selected?.map(({ name, key }) => ({
      label: columnLabelOf(name, key),
      value: columnLabelOf(name, key),
    })) || [];
  const options = columns.map((col) => ({
    label: columnLabelOf(col.name, col.key),
    value: columnLabelOf(col.name, col.key),
  }));

  const getColumn = (label: string | undefined): Column | undefined => {
    return columns.find(({ name, key }) => columnLabelOf(name, key) === label);
  };

  useEffect(() => {
    const valid = selected?.filter((col) => getColumn(columnLabelOf(col.name, col.key))) || [];
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
        options={options}
        value={selectOptions}
        disabled={disabled}
        isLoading={isLoading}
        onChange={(item: Array<SelectableValue<string>>) => {
          const newSelected = item
            .map((v) => getColumn(v.label))
            .map<ComplexField>((col) => ({ name: col?.name || '', key: col?.key || undefined }))
            .filter(({ name }) => name);
          onChange(newSelected);
        }}
      />
    </div>
  );
}
