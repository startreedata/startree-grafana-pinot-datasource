import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { PinotQuery } from '../types/sql';
import { PinotConnectionConfig } from '../types/config';
import React, { useEffect, useState } from 'react';
import { InlineField, InlineFieldRow, MultiSelect, Select } from '@grafana/ui';

type TimeSeriesProps = QueryEditorProps<DataSource, PinotQuery, PinotConnectionConfig>;

interface GetTablesResponse {
  tables: string[];
}

interface GetTableSchemaResponse {
  schema: TableSchema;
}

interface TableSchema {
  schemaName: string;
  dimensionFieldSpecs: DimensionFieldSpec[];
  metricFieldSpecs: MetricFieldSpec[];
  dateTimeFieldSpecs: DateTimeFieldSpec[];
}

interface DimensionFieldSpec {
  name: string;
  dataType: string;
}

interface MetricFieldSpec {
  name: string;
  dataType: string;
}

interface DateTimeFieldSpec {
  name: string;
  dataType: string;
  format: string;
  granularity: string;
}

function usePinotTables(props: TimeSeriesProps): string[] {
  const [tables, setTables] = useState<string[]>([]);
  useEffect(() => {
    props.datasource.getResource<GetTablesResponse>('tables').then((resp) => setTables(resp.tables));
  }, [0]);
  return tables;
}

function useTableSchema(props: TimeSeriesProps, tableName?: string) {
  const [tableSchema, setTableSchema] = useState<TableSchema | null>(null);
  useEffect(() => {
    if (tableName) {
      props.datasource
        .getResource<GetTableSchemaResponse>('tables/' + tableName + '/schema')
        .then((resp) => setTableSchema(resp.schema));
    }
  }, [tableName]);
  return tableSchema;
}

export function TimeSeriesQueryEditor(props: TimeSeriesProps) {
  const { query, onChange } = props;

  const onTimeColumnChange = (value: SelectableValue<string>) => {
    props.onChange({ ...query, timeColumn: value.value });
  };

  const onMetricColumnChange = (value: SelectableValue<string>) => {
    onChange({ ...query, metricColumn: value.value });
  };

  const onAggregationFunctionChange = (value: SelectableValue<string>) => {
    onChange({ ...query, aggregationFunction: value.value });
  };

  const onTableNameChange = (value: SelectableValue<string>) => {
    onChange({ ...query, tableName: value.value, timeColumn: undefined, metricColumn: undefined });
  };

  const onDimensionSelect = (item: SelectableValue<string>[]) => {
    const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
    console.log({ message: 'updating selected columns', value: selected });
    onChange({ ...query, dimensionColumns: selected });
  };

  const tables = usePinotTables(props);
  const schema = useTableSchema(props, query.tableName);
  const aggFunctions = ['sum', 'count'];

  const timeColumns = schema?.dateTimeFieldSpecs.map((spec) => spec.name);
  const metricColumns = schema?.metricFieldSpecs.map((spec) => spec.name);
  const dimensionColumns = schema?.dimensionFieldSpecs.map((spec) => spec.name);

  return (
    <div className="gf-form">
      <InlineFieldRow>
        <InlineField label="Table" tooltip="Table name">
          <Select
            options={tables.map((name) => ({ label: name, value: name }))}
            value={query.tableName}
            onChange={onTableNameChange}
          />
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
        <InlineField label="Time Column" labelWidth={16} tooltip="Supply time column">
          <Select
            options={(timeColumns || []).map((name) => ({ label: name, value: name }))}
            value={query.timeColumn}
            onChange={onTimeColumnChange}
          />
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
        <InlineField label="Metric Column" labelWidth={16} tooltip="Supply time column">
          <Select
            options={(metricColumns || []).map((name) => ({ label: name, value: name }))}
            value={query.metricColumn}
            onChange={onMetricColumnChange}
          />
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
        <InlineField label="Dimesions" labelWidth={16} tooltip="Select dimensions">
          <MultiSelect
            options={(dimensionColumns || []).map((name) => ({ label: name, value: name }))}
            value={query.dimensionColumns}
            onChange={onDimensionSelect}
          />
        </InlineField>
      </InlineFieldRow>

      <InlineFieldRow>
        <InlineField label="Agg Function" labelWidth={16} tooltip="Supply time column">
          <Select
            options={aggFunctions.map((name) => ({ label: name, value: name }))}
            value={query.aggregationFunction}
            onChange={onAggregationFunctionChange}
          />
        </InlineField>
      </InlineFieldRow>
    </div>
  );
}
