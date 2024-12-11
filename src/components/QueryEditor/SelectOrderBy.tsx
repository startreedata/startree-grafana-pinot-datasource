import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import React from 'react';
import { OrderByClause } from '../../types/OrderByClause';
import { styles } from '../../styles';
import { SelectableValue } from '@grafana/data';
import { columnLabelOf, ComplexField } from '../../types/ComplexField';

export function SelectOrderBy(props: {
  selected: OrderByClause[] | undefined;
  columns: ComplexField[] | undefined;
  disabled: boolean;
  onChange: (val: OrderByClause[] | undefined) => void;
}) {
  const { columns, selected, disabled, onChange } = props;
  const labels = allLabels.components.QueryEditor.orderBy;

  const clauseToLabel = ({ columnName, columnKey, direction }: OrderByClause) =>
    `${columnLabelOf(columnName, columnKey)} ${direction.toLowerCase()}`;

  const usedOptions = (selected || []).map((clause) => ({
    label: clauseToLabel(clause),
    value: clauseToLabel(clause),
    clause,
  }));

  const usedLabels = new Set(usedOptions.map(({ label }) => label));

  const unusedOptions = (columns || [])
    .filter((col) => !usedLabels.has(columnLabelOf(col.name, col.key)))
    .flatMap<OrderByClause>((col) => [
      { columnName: col.name, columnKey: col.key || undefined, direction: 'ASC' },
      { columnName: col.name, columnKey: col.key || undefined, direction: 'DESC' },
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
