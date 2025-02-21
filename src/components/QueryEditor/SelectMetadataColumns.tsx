import {
  ComplexField,
  findComplexFieldByLabel,
  optionsOf,
  selectableComplexFieldsOf,
} from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import React from 'react';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';

export function SelectMetadataColumns(props: {
  selected: ComplexField[];
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField[]) => void;
}) {
  const { columns, selected, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metadataColumns;

  const complexFields = selectableComplexFieldsOf(selected, columns);
  return (
    <div className={'gf-form'} data-testid="select-metadata">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-metadata-dropdown">
        <MultiSelect
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          options={optionsOf(complexFields)}
          value={optionsOf(selected)}
          isLoading={isLoading}
          onChange={(item: Array<SelectableValue<string>>) =>
            onChange(item.map((v) => findComplexFieldByLabel(v.label, complexFields)).filter(({ name }) => name))
          }
        />
      </div>
    </div>
  );
}
