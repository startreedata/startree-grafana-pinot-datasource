import React from 'react';
import { AccessoryButton, InputGroup } from '@grafana/experimental';
import { MultiSelect, Select } from '@grafana/ui';
import { DimensionFilter, PinotDataType, PinotDataTypes, TableSchema, useDistinctValues } from '../resources/resources';
import { DataSource } from '../datasource';
import { SelectableValue, TimeRange } from '@grafana/data';
import { FormLabel } from './FormLabel';
import allLabels from '../labels';

export function SelectFilters(props: {
  datasource: DataSource;
  databaseName: string | undefined;
  tableSchema: TableSchema | undefined;
  tableName: string | undefined;
  timeColumn: string | undefined;
  range: TimeRange | undefined;
  dimensionColumns: string[] | undefined;
  dimensionFilters: DimensionFilter[];
  onChange: (val: DimensionFilter[]) => void;
}) {
  const { dimensionColumns, dimensionFilters, onChange } = props;
  const labels = allLabels.components.QueryEditor.filters;

  const filteredColumns = dimensionFilters?.map((f) => f.columnName) || [];
  const unusedColumns = dimensionColumns?.filter((d) => !filteredColumns.includes(d));

  const onChangeFilter = (val: DimensionFilter, idx: number) => {
    onChange(dimensionFilters.map((existing, i) => (i === idx ? val : existing)));
  };
  const onDeleteFilter = (idx: number) => {
    onChange(dimensionFilters.filter((val, i) => i !== idx));
  };

  return (
    <div className={'gf-form'}>
      <FormLabel tooltip={labels.tooltip} label={labels.label} />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {dimensionFilters.map((filter, idx) => (
          <DimensionFilterEditor
            {...props}
            key={idx}
            unusedColumns={unusedColumns}
            thisFilter={filter}
            remainingFilters={[...dimensionFilters.slice(0, idx), ...dimensionFilters.slice(idx + 1)]}
            onChange={(val: DimensionFilter) => onChangeFilter(val, idx)}
            onDelete={() => onDeleteFilter(idx)}
          />
        ))}
        <div>
          <AccessoryButton
            icon="plus"
            variant="secondary"
            fullWidth={false}
            onClick={() => {
              onChange([...(dimensionFilters || []), {}]);
            }}
          />
        </div>
      </div>
    </div>
  );
}

const operators = [
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

  const operatorOptions = columnType ? operators.filter((op) => op.types.includes(columnType)) : operators;
  const valueOptions = values?.map((val) => ({ label: val, value: val }));

  const operatorIsMulti = operators.find((op) => op.value == thisFilter.operator)?.multi || false;

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
            operator: thisFilter.operator ?? DefaultOperator.value,
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
              operator: thisFilter.operator ?? DefaultOperator.value,
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
                operator: thisFilter.operator ?? DefaultOperator.value,
              });
            }
          }}
        />
      )}

      <AccessoryButton icon="times" variant="secondary" onClick={onDelete} />
    </InputGroup>
  );
}
