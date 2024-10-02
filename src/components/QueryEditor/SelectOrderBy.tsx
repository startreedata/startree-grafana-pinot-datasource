import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import React from 'react';
import { OrderByClause } from '../../types/OrderByClause';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';

export function SelectOrderBy(props: {
  selected: OrderByClause[] | undefined;
  columnNames: string[] | undefined;
  disabled: boolean;
  onChange: (val: OrderByClause[] | undefined) => void;
}) {
  const { columnNames, selected, disabled, onChange } = props;
  const labels = allLabels.components.QueryEditor.orderBy;

  const clauseToLabel = ({ columnName, direction }: OrderByClause) => `${columnName} ${direction.toLowerCase()}`;

  const usedOptions = (selected || []).map((clause) => ({
    label: clauseToLabel(clause),
    value: clauseToLabel(clause),
    clause,
  }));

  const usedColumns = new Set((selected || []).map(({ columnName }) => columnName));
  const unusedOptions = (columnNames || [])
    .filter((columnNames) => !usedColumns.has(columnNames))
    .flatMap((val) => [
      { columnName: val, direction: 'ASC' },
      { columnName: val, direction: 'DESC' },
    ])
    .map((clause) => ({
      label: clauseToLabel(clause),
      value: clauseToLabel(clause),
      clause,
    }));

  const options = [...usedOptions, ...unusedOptions];

  return (
    <div className={'gf-form'} data-testid="select-order-by">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <MultiSelect
        className={`${styles.QueryEditor.inputForm}`}
        disabled={disabled}
        options={options}
        value={usedOptions}
        onChange={(item: Array<SelectableValue<string>>) => {
          onChange(
            item
              .map(({ value }) => options.find((opt) => opt.value === value)?.clause)
              .filter((clause) => clause !== undefined) as OrderByClause[]
          );
        }}
      />
    </div>
  );
}
