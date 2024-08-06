import allLabels from '../../labels';
import { FormLabel } from './FormLabel';
import { Select } from '@grafana/ui';
import React from 'react';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { DimensionFilter } from '../../types/DimensionFilter';
import { OrderByClause } from '../../types/OrderByClause';

const directionOptions = [
  { label: 'ASC', value: 'ASC' },
  { label: 'DESC', value: 'DESC' },
];

export function SelectOrderBy(props: {
  selected: OrderByClause[];
  options: string[] | undefined;
  onChange: (val: OrderByClause[] | undefined) => void;
}) {
  const { options, selected, onChange } = props;
  const labels = allLabels.components.QueryEditor.orderBy;

  const selectedColumns = (selected || [])
    .map(({ columnName }) => columnName || '')
    .filter((columnName) => columnName)
    .reduce((collector, val) => collector.add(val), new Set<string>());

  const unused = (options || []).filter((val) => !selectedColumns.has(val));

  const onChangeClause = (val: DimensionFilter, idx: number) => {
    onChange(selected.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteClause = (idx: number) => {
    onChange(selected.filter((val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {selected?.map((clause, idx) => (
          <EditOrderBy
            key={idx}
            clause={clause}
            options={clause.columnName ? [clause.columnName, ...unused] : unused}
            onChange={(val: OrderByClause) => onChangeClause(val, idx)}
            onDelete={() => onDeleteClause(idx)}
          />
        ))}
        <div>
          <AccessoryButton
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(selected || []), { direction: 'ASC' }]);
            }}
          />
        </div>
      </div>
    </div>
  );
}

function EditOrderBy(props: {
  clause: OrderByClause;
  options: string[];
  onChange: (val: OrderByClause) => void;
  onDelete: () => void;
}) {
  const { clause, options, onChange, onDelete } = props;
  return (
    <InputGroup>
      <Select
        width="auto"
        value={clause.columnName}
        allowCustomValue
        options={options.map((k) => ({ label: k, value: k }))}
        onChange={(change) => {
          onChange({
            ...clause,
            columnName: change.value,
          });
        }}
      />

      <Select
        value={clause.direction}
        options={directionOptions}
        width="auto"
        onChange={(change) => {
          onChange({
            ...clause,
            direction: change.value,
          });
        }}
      />

      <AccessoryButton icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
