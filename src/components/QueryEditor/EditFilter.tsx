import { DataSource } from '../../datasource';
import { DateTime, SelectableValue } from '@grafana/data';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { MultiSelect, Select } from '@grafana/ui';
import React, { useState } from 'react';
import { PinotDataType, PinotDataTypes } from '../../dataquery/PinotDataType';
import { DimensionFilter } from '../../dataquery/DimensionFilter';
import { queryDistinctValuesForFilters } from '../../resources/distinctValues';
import { Column } from '../../resources/columns';
import { columnLabelOf } from '../../dataquery/ComplexField';

const FilterOperators = [
  { label: '=', value: '=', types: PinotDataTypes, multi: true },
  { label: '!=', value: '!=', types: PinotDataTypes, multi: true },
  { label: '>', value: '>', types: PinotDataTypes, multi: false },
  { label: '>=', value: '>=', types: PinotDataTypes, multi: false },
  { label: '<', value: '<', types: PinotDataTypes, multi: false },
  { label: '<=', value: '<=', types: PinotDataTypes, multi: false },
  {
    label: 'like',
    value: 'like',
    types: [PinotDataType.STRING],
    multi: false,
  },
  {
    label: 'not like',
    value: 'not like',
    types: [PinotDataType.STRING],
    multi: false,
  },
];

const DefaultFilterOperator = FilterOperators[0];

export function EditFilter(props: {
  datasource: DataSource;
  remainingFilters: DimensionFilter[];
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  tableName: string | undefined;
  thisFilter: DimensionFilter;
  thisColumn: Column | undefined;
  timeColumn: string | undefined;
  unusedColumns: Column[];
  isLoadingColumns: boolean;
  onChange: (filter: DimensionFilter) => void;
  onDelete: () => void;
}) {
  const {
    datasource,
    remainingFilters,
    timeRange,
    tableName,
    timeColumn,
    thisFilter,
    thisColumn,
    unusedColumns,
    isLoadingColumns,
    onChange,
    onDelete,
  } = props;

  const filterableColumns = thisColumn ? [thisColumn, ...unusedColumns] : unusedColumns;
  const colOptions = filterableColumns
    .map(({ name, key }) => columnLabelOf(name, key))
    .filter((d, i, a) => a.indexOf(d) === i)
    .map((label) => ({ label, value: label }));

  const operatorOptions = thisColumn?.dataType
    ? FilterOperators.filter((op) => op.types.includes(thisColumn.dataType))
    : FilterOperators;
  const operatorIsMulti = FilterOperators.find((op) => op.value === thisFilter.operator)?.multi || false;

  const [distinctValues, setDistinctValues] = useState<string[]>();
  const [isLoadingValues, setIsLoadingValues] = useState(false);
  const loadValueOptions = () => {
    setIsLoadingValues(true);
    queryDistinctValuesForFilters(datasource, {
      tableName: tableName,
      columnName: thisColumn?.name,
      columnKey: thisColumn?.key || undefined,
      timeColumn: timeColumn,
      timeRange: timeRange,
      filters: remainingFilters,
    })
      .then((vals) => setDistinctValues(vals))
      .then(() => setIsLoadingValues(false));
  };

  const valueOptions = [...(thisFilter.valueExprs || []), ...(distinctValues || [])]
    .filter((v, i, a) => a.indexOf(v) === i)
    .map((val) => ({ label: val, value: val }));

  return (
    <InputGroup>
      <div data-testid="query-filter-column-select">
        <Select
          placeholder="Select column"
          width="auto"
          value={thisFilter.columnName}
          allowCustomValue
          options={colOptions}
          isLoading={isLoadingColumns}
          onChange={(change) => {
            const col = filterableColumns.find(({ name, key }) => columnLabelOf(name, key) === change.label);
            onChange({
              ...thisFilter,
              columnName: col?.name,
              columnKey: col?.key || undefined,
              operator: thisFilter.operator ?? DefaultFilterOperator.value,
            });
          }}
        />
      </div>

      <div data-testid="query-filter-operator-select">
        <Select
          className="query-segment-operator"
          value={thisFilter.operator}
          options={operatorOptions}
          width="auto"
          onChange={(change) => {
            onChange({
              ...thisFilter,
              operator: change.value,
            });
          }}
        />
      </div>

      {operatorIsMulti ? (
        <div data-testid="query-filter-value-select">
          <MultiSelect
            placeholder="Select value"
            width="auto"
            isLoading={isLoadingValues}
            value={thisFilter.valueExprs?.map((v) => ({ label: v, value: v }))}
            allowCustomValue
            options={valueOptions}
            onOpenMenu={() => loadValueOptions()}
            onChange={(change: Array<SelectableValue<string>>) => {
              const selected = change.map((v) => v.value).filter((v) => v !== undefined) as string[];
              onChange({
                ...thisFilter,
                valueExprs: selected,
                operator: thisFilter.operator ?? DefaultFilterOperator.value,
              });
            }}
          />
        </div>
      ) : (
        <div data-testid="query-filter-value-select">
          <Select
            placeholder="Select value"
            width="auto"
            value={thisFilter.valueExprs?.find((v, i) => i === 0)}
            onOpenMenu={() => loadValueOptions()}
            isLoading={isLoadingValues}
            allowCustomValue
            options={valueOptions}
            onChange={(change: SelectableValue<string>) => {
              if (change.value) {
                onChange({
                  ...thisFilter,
                  valueExprs: [change.value],
                  operator: thisFilter.operator ?? DefaultFilterOperator.value,
                });
              }
            }}
          />
        </div>
      )}

      <AccessoryButton data-testid="delete-filter-btn" icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
