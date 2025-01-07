import allLabels from '../../labels';
import { RadioButtonGroup } from '@grafana/ui';
import React from 'react';
import { FormLabel } from './FormLabel';

export const VariableType = Object.freeze({
  TableList: 'TABLE_LIST',
  ColumnList: 'COLUMN_LIST',
  DistinctValues: 'DISTINCT_VALUES',
  PinotQlCode: 'PINOT_QL_CODE',
});

export function SelectVariableType(props: { selected: string; onChange: (val: string) => void }) {
  const { selected, onChange } = props;
  const labels = allLabels.components.VariableQueryEditor.variableType;
  const options = [
    { label: 'Tables', value: VariableType.TableList },
    { label: 'Columns', value: VariableType.ColumnList },
    { label: 'Distinct Values', value: VariableType.DistinctValues },
    { label: 'Sql Query', value: VariableType.PinotQlCode },
  ];

  return (
    <>
      <FormLabel label={labels.label} tooltip={labels.tooltip} />
      <RadioButtonGroup options={options} onChange={(val) => onChange(val)} value={selected} />
    </>
  );
}
