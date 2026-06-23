import React from 'react';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { Select } from '@grafana/ui';
import { Column } from '../../resources/columns';
import { Aggregation } from '../../dataquery/Aggregation';
import { AggregationFunction } from './SelectAggregation';
import { formDataOf } from '../../pinotql/complexField';

const FunctionOptions = Object.values(AggregationFunction)
  .filter((fn) => fn !== AggregationFunction.NONE)
  .map((fn) => ({ label: fn, value: fn }));

export function EditAggregation(props: {
  aggregation: Aggregation;
  columns: Column[];
  isLoadingColumns: boolean;
  onChange: (v: Aggregation) => void;
  onDelete: () => void;
}) {
  const { aggregation, columns, isLoadingColumns, onChange, onDelete } = props;
  const isCount = aggregation.function === AggregationFunction.COUNT;
  const columnFormData = formDataOf(aggregation.column || {}, columns);
  return (
    <InputGroup data-testid="edit-aggregation">
      <div data-testid="aggregation-select-function">
        <Select
          placeholder="Function"
          width="auto"
          allowCustomValue
          invalid={!aggregation.function}
          options={FunctionOptions}
          value={aggregation.function}
          onChange={(change) => onChange({ ...aggregation, function: change.value })}
        />
      </div>
      <div data-testid="aggregation-select-column">
        <Select
          placeholder={isCount ? '*' : 'Column'}
          width="auto"
          allowCustomValue
          isLoading={isLoadingColumns}
          options={columnFormData.options}
          value={isCount && !aggregation.column?.name ? { label: '*', value: '*' } : columnFormData.usedOption}
          onChange={(item) => {
            const col = columnFormData.getChange(item);
            onChange({ ...aggregation, column: { name: col?.name, key: col?.key || undefined } });
          }}
        />
      </div>
      <AccessoryButton data-testid="delete-aggregation-btn" icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
