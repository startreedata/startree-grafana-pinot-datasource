import { ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import React from 'react';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { multiSelectFormDataOf } from '../../pinotql/complexField';

export function SelectMetadataColumns(props: {
  selected: ComplexField[];
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField[]) => void;
}) {
  const { columns, selected, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metadataColumns;
  const formData = multiSelectFormDataOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid="select-metadata">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-metadata-dropdown">
        <MultiSelect
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          options={formData.options}
          value={formData.usedOptions}
          isLoading={isLoading}
          onChange={(items) => onChange(formData.getChange(items))}
        />
      </div>
    </div>
  );
}
