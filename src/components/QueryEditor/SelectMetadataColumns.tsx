import { columnLabelOf, ComplexField } from '../../dataquery/ComplexField';
import { Column } from '../../resources/columns';
import allLabels from '../../labels';
import React, { useEffect } from 'react';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';

export function SelectMetadataColumns(props: {
  selected: ComplexField[] | undefined;
  columns: Column[];
  isLoading: boolean;
  onChange: (val: ComplexField[]) => void;
}) {
  const { columns, selected, isLoading, onChange } = props;
  const labels = allLabels.components.QueryEditor.metadataColumns;

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
    <div className={'gf-form'} data-testid="select-metadata">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-metadata-dropdown">
        <MultiSelect
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue
          options={options}
          value={selectOptions}
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
    </div>
  );
}
