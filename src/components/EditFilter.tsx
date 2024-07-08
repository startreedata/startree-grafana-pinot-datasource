import { DataSource } from '../datasource';
import { DimensionFilter, PinotDataType, PinotDataTypes, TableSchema, useDistinctValues } from '../resources/resources';
import { SelectableValue, TimeRange } from '@grafana/data';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { MultiSelect, Select } from '@grafana/ui';
import React from 'react';

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
  databaseName: string | undefined;
  datasource: DataSource;
  remainingFilters: DimensionFilter[];
  range: TimeRange | undefined;
  tableName: string | undefined;
  tableSchema: TableSchema | undefined;
  thisFilter: DimensionFilter;
  timeColumn: string | undefined;
  unusedColumns: string[] | undefined;
  onChange: (filter: DimensionFilter) => void;
  onDelete: () => void;
}) {
  const {
    databaseName,
    datasource,
    remainingFilters,
    range,
    tableName,
    tableSchema,
    timeColumn,
    thisFilter,
    unusedColumns,
    onChange,
    onDelete,
  } = props;

  const values = useDistinctValues(datasource, {
    databaseName: databaseName,
    tableName: tableName,
    columnName: thisFilter.columnName,
    timeColumn: timeColumn,
    timeRange: { from: range?.from, to: range?.to },
    filters: remainingFilters,
  });

  const columnType = [
    ...(tableSchema?.dateTimeFieldSpecs || []),
    ...(tableSchema?.dimensionFieldSpecs || []),
    ...(tableSchema?.metricFieldSpecs || []),
  ].find((spec) => spec.name == thisFilter.columnName)?.dataType;

  const dimOptions = unusedColumns
    ? [thisFilter.columnName, ...unusedColumns]
        .filter((d, i, a) => a.indexOf(d) == i)
        .map((col) => ({
          label: col,
          value: col,
        }))
    : undefined;

  const operatorOptions = columnType ? FilterOperators.filter((op) => op.types.includes(columnType)) : FilterOperators;
  const valueOptions = values?.map((val) => ({ label: val, value: val }));

  const operatorIsMulti = FilterOperators.find((op) => op.value == thisFilter.operator)?.multi || false;

  return (
    <InputGroup>
      <Select
        placeholder="Select column"
        width="auto"
        value={thisFilter.columnName}
        allowCustomValue
        options={dimOptions}
        onChange={(change) => {
          onChange({
            ...thisFilter,
            columnName: change.value,
            operator: thisFilter.operator ?? DefaultFilterOperator.value,
          });
        }}
      />

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

      {operatorIsMulti ? (
        <MultiSelect
          placeholder="Select value"
          width="auto"
          value={thisFilter.valueExprs}
          options={valueOptions}
          allowCustomValue
          onChange={(change: SelectableValue<string>[]) => {
            const selected = change.map((v) => v.value).filter((v) => v !== undefined) as string[];
            onChange({
              ...thisFilter,
              valueExprs: selected,
              operator: thisFilter.operator ?? DefaultFilterOperator.value,
            });
          }}
        />
      ) : (
        <Select
          placeholder="Select value"
          width="auto"
          value={thisFilter.valueExprs?.find((v, i) => i === 0)}
          options={valueOptions}
          allowCustomValue
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
      )}

      <AccessoryButton icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
