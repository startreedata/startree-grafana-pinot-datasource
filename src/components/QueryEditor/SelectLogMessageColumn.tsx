import { ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import { styles } from '../../styles';
import React from 'react';
import { formDataOf } from '../../pinotql/complexField';

export function SelectLogMessageColumn(props: {
  selected: ComplexField;
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField) => void;
}) {
  const { selected, columns, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.logColumn;
  const formData = formDataOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid="select-log-column">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <Select
        className={`${styles.QueryEditor.inputForm}`}
        allowCustomValue
        invalid={!selected}
        isLoading={isLoading}
        options={formData.options}
        value={formData.usedOption}
        onChange={(item) => onChange(formData.getChange(item))}
      />
    </div>
  );
}
