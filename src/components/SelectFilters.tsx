import React from 'react';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { MultiSelect, Select } from '@grafana/ui';
import { DimensionFilter, TableSchema, useDistinctValues } from '../resources/resources';
import { DataSource } from '../datasource';
import { SelectableValue, TimeRange } from '@grafana/data';
import { FormLabel } from './FormLabel';

export function SelectFilters(props: {
  datasource: DataSource;
  databaseName: string | undefined;
  tableSchema: TableSchema | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  range: TimeRange | undefined;
  dimensionColumns: string[] | undefined;
  dimensionFilters: DimensionFilter[] | undefined;
  onChange: (val: DimensionFilter[]) => void;
}) {
  const { dimensionColumns, dimensionFilters, onChange } = props;

  const filteredDims = dimensionFilters?.map((f) => f.columnName) || [];
  const unusedColumns = dimensionColumns?.filter((d) => !filteredDims.includes(d)) || [];

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={'Select group by filters'} label={'Filters'} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {dimensionFilters?.map((f, i) => (
          <DimensionFilterEditor
            {...props}
            key={f.columnName}
            unusedColumns={unusedColumns}
            thisFilter={f}
            remainingFilters={[...dimensionFilters].splice(i, 1)}
            onChange={(val: DimensionFilter) => onChange([...dimensionFilters].splice(i, 1, val))}
            onDelete={() => onChange([...dimensionFilters].splice(i, 1))}
          />
        ))}
      </div>
      <AccessoryButton
          icon="plus"
          variant="secondary"
          onClick={() => {
            onChange([...(dimensionFilters || []), {}]);
          }}
      />
    </div>
  );
}

const operators = [
  { label: '=', value: '=', types: ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'] },
  { label: '!=', value: '!=', types: ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'STRING'] },
  {
    label: 'like',
    value: 'like',
    types: ['STRING'],
  },
  {
    label: 'not like',
    value: 'not like',
    types: ['STRING'],
  },
];

const DefaultOperator = operators[0];

export function DimensionFilterEditor(props: {
  databaseName: string | undefined;
  datasource: DataSource;
  remainingFilters: DimensionFilter[];
  range: TimeRange | undefined;
  tableName: string | undefined;
  tableSchema: TableSchema | undefined;
  thisFilter: DimensionFilter;
  timeColumn: string | undefined;
  unusedColumns: string[];
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
    dimensionFilters: remainingFilters,
  });

  const columnType = [
    ...(tableSchema?.dateTimeFieldSpecs || []),
    ...(tableSchema?.dimensionFieldSpecs || []),
    ...(tableSchema?.metricFieldSpecs || []),
  ].find((spec) => spec.name == thisFilter.columnName)?.dataType;

  const dimOptions = [thisFilter.columnName, ...unusedColumns]
    .filter((d, i, a) => a.indexOf(d) == 0)
    .map((col) => ({
      label: col,
      value: col,
    }));

  const operatorOptions = columnType ? operators.filter((op) => op.types.includes(columnType)) : operators;
  const valueOptions = values?.map((val) => ({ label: val, name: val }));

  return (
    <InputGroup>
      <Select
        placeholder="Select column"
        width="auto"
        value={thisFilter.columnName}
        allowCustomValue
        options={dimOptions}
        onChange={(change) => {
          if (change.label) {
            onChange({
              ...thisFilter,
              columnName: change.label,
              operation: thisFilter.operation ?? DefaultOperator.value,
            });
          }
        }}
      />

      <Select
        className="query-segment-operator"
        value={thisFilter.operation}
        options={operatorOptions}
        width="auto"
        onChange={(change) => {
          if (change.value != null) {
            onChange({
              ...thisFilter,
              operation: change.value,
            });
          }
        }}
      />

      <MultiSelect
        placeholder="Select value"
        width="auto"
        value={thisFilter.valueExprs}
        allowCustomValue
        options={valueOptions}
        onChange={(change: SelectableValue<string>[]) => {
          if (change.values()) {
            onChange({
              ...thisFilter,
              valueExprs: change.map((v) => v.value).filter((v) => v !== undefined) as string[],
              operation: thisFilter.operation ?? DefaultOperator.value,
            });
          }
        }}
      />
      <AccessoryButton icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
