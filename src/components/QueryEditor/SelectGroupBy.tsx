import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { FormLabel } from './FormLabel';
import allLabels from '../../labels';
import { ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import { multiSelectFormDataOf } from '../../pinotql/complexField';

export function SelectGroupBy(props: {
  selected: ComplexField[];
  columns: Column[];
  isLoading: boolean;
  disabled: boolean;
  onChange: (val: ComplexField[]) => void;
}) {
  const { columns, selected, disabled, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.groupBy;
  const formData = multiSelectFormDataOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid="select-group-by">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-group-by-dropdown">
        <MultiSelect
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          options={formData.options}
          value={formData.usedOptions}
          disabled={disabled}
          isLoading={isLoading}
          onChange={(item) => onChange(formData.getChange(item))}
        />
      </div>
    </div>
  );
}
