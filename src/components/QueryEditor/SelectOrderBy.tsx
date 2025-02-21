import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { MultiSelect } from '@grafana/ui';
import React from 'react';
import { OrderByClause } from '../../dataquery/OrderByClause';
import { styles } from '../../styles';
import { ComplexField } from '../../dataquery/ComplexField';
import { formDataOf } from '../../pinotql/orderBy';

export function SelectOrderBy(props: {
  selected: OrderByClause[];
  columns: ComplexField[];
  disabled: boolean;
  onChange: (val: OrderByClause[]) => void;
}) {
  const { columns, selected, disabled, onChange } = props;
  const labels = allLabels.components.QueryEditor.orderBy;
  const formData = formDataOf(selected, columns);

  return (
    <div className={'gf-form'} data-testid="select-order-by">
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div data-testid="select-order-by-dropdown">
        <MultiSelect
          className={`${styles.QueryEditor.inputForm}`}
          allowCustomValue={true}
          disabled={disabled}
          options={formData.options}
          value={formData.usedOptions}
          onChange={(items) => onChange(formData.getChange(items))}
        />
      </div>
    </div>
  );
}
