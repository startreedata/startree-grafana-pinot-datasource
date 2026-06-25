import { ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { formDataOf } from '../../pinotql/complexField';

export function SelectLevelColumn(props: {
  selected: ComplexField;
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField) => void;
}) {
  const { selected, columns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.levelColumn;
  const formData = formDataOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid="select-level-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        isClearable
        isLoading={isLoading}
        options={formData.options}
        value={formData.usedOption}
        onChange={(item) => onChange(formDataOf(selected, columns).getChange(item ?? {}))}
      />
    </div>
  );
}
