import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from '../datasource';
import { PinotConnectionConfig } from '../types/config';
import { Field, InlineFormLabel, MultiSelect, RadioButtonGroup, Select, TextArea } from '@grafana/ui';
import { useDatabases, useSqlPreview, useTables, useTableSchema } from '../resources/resources';
import React from 'react';
import { DataQuery } from '@grafana/schema';
import { styles } from '../styles';

type PinotQueryEditorProps = QueryEditorProps<DataSource, PinotDataQuery, PinotConnectionConfig>;

export enum QueryType {
  PinotQL = 'PinotQL',
  PromQL = 'PromQL',
  LogQL = 'LogQL',
}

export enum EditorMode {
  Builder = 'Builder',
  Code = 'Code',
}

export const GetDefaultPinotDataQuery = (): Partial<PinotDataQuery> => ({
  queryType: QueryType.PinotQL,
  editorMode: EditorMode.Builder,
  databaseName: 'default',
});

export interface PinotDataQuery extends DataQuery {
  queryType?: string;
  editorMode?: string;
  rawSql?: string;
  databaseName?: string;
  tableName?: string;
  timeColumn?: string;
  metricColumn?: string;
  dimensionColumns?: string[];
  aggregationFunction?: string;
  limit?: number;
}

export function PinotQueryEditor(props: PinotQueryEditorProps) {
  return (
    <div>
      <SelectEditorType {...props} />
      {SubEditor(props)}
    </div>
  );
}

function SelectEditorType(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  return (
    <div className={'gf-form'} style={{ display: 'flex', justifyContent: 'space-between' }}>
      <div className={'gf-form'}>
        <InlineFormLabel width={8} className="query-keyword" tooltip={'Select query type'}>
          Query Type
        </InlineFormLabel>
        <RadioButtonGroup
          options={Object.keys(QueryType).map((name) => ({ label: name, value: name }))}
          onChange={(value) => {
            // Manually disable unimplemented options
            switch (value) {
              case QueryType.LogQL:
              case QueryType.PromQL:
                // TODO: Add some unsupported popup
                return;
            }
            onChange({ ...query, queryType: value });
          }}
          value={query.queryType}
        />
      </div>
      <div className={'gf-form'}>
        <RadioButtonGroup
          options={Object.keys(EditorMode).map((name) => ({ label: name, value: name }))}
          onChange={(value) => onChange({ ...query, editorMode: value })}
          value={query.editorMode}
        />
      </div>
    </div>
  );
}

function SubEditor(props: PinotQueryEditorProps) {
  switch (props.query.queryType) {
    case QueryType.PinotQL:
    default:
      return PinotQlEditor(props);
  }
}

function PinotQlEditor(props: PinotQueryEditorProps) {
  return (
    <div className={'gf-form'} style={{ display: 'flex', flexDirection: 'column' }}>
      <div className={'gf-form'}>
        <SelectDatabase {...props} />
        <SelectTable {...props} />
      </div>
      {props.query.editorMode == EditorMode.Code ? (
        <PinotQlCodeEditor {...props} />
      ) : (
        <PinotQlBuilderEditor {...props} />
      )}
    </div>
  );
}

function SelectDatabase(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;
  const databases = useDatabases(datasource);
  const defaultDatabase = GetDefaultPinotDataQuery().databaseName;

  const onChangeDatabase = (value: string | undefined) =>
    onChange({
      ...query,
      databaseName: value,
      tableName: undefined,
      timeColumn: undefined,
      metricColumn: undefined,
      aggregationFunction: undefined,
      dimensionColumns: undefined,
    });

  if (databases.length == 1 && query.databaseName == undefined) {
    onChangeDatabase(databases[0]);
  }

  // TODO: Use AsyncSelect
  return (
    <>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select Pinot database'}>
        Database
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        // TODO: Handle the default db name correctly.
        options={[defaultDatabase, ...databases].map((name) => ({ label: name, value: name }))}
        value={query.databaseName || defaultDatabase}
        disabled={[0, 1].includes(databases.length)}
        onChange={(value: SelectableValue<string>) => onChangeDatabase(value.value)}
      />
    </>
  );
}

function SelectTable(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  const tables = useTables(datasource, query.databaseName);

  // TODO: Use AsyncSelect
  return (
    <>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select Pinot table'}>
        Table
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={tables.map((name) => ({ label: name, value: name }))}
        value={query.tableName}
        onChange={(value) =>
          onChange({
            ...query,
            tableName: value.value,
            timeColumn: undefined,
            metricColumn: undefined,
            aggregationFunction: undefined,
            dimensionColumns: undefined,
          })
        }
      />
    </>
  );
}

function PinotQlCodeEditor(props: PinotQueryEditorProps) {
  return (
    <Field label="Sql Query">
      <TextArea
        cols={500}
        onChange={(value) => props.onChange({ ...props.query, rawSql: value.currentTarget.innerText })}
      />
    </Field>
  );
}

function PinotQlBuilderEditor(props: PinotQueryEditorProps) {
  return (
    <>
      <div>
        <SelectTimeColumn {...props} />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetric {...props} />
        <SelectAggregation {...props} />
      </div>
      <div>
        <SelectGroupBy {...props} />
      </div>
      <div>
        <SqlPreview {...props} />
      </div>
    </>
  );
}

function SelectTimeColumn(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  // TODO: Pass this as a param
  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const timeColumns = schema?.dateTimeFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select time column'}>
        Time Column
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(timeColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.timeColumn}
        onChange={(value) => onChange({ ...query, timeColumn: value.value })}
      />
    </div>
  );
}

function SelectMetric(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const metricColumns = schema?.metricFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select metric column'}>
        Metric Column
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(metricColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.metricColumn}
        onChange={(value) => onChange({ ...query, metricColumn: value.value })}
      />
    </div>
  );
}

function SelectAggregation(props: PinotQueryEditorProps) {
  const { query, onChange } = props;

  // TODO: Where do these belong more permanently?
  const aggFunctions = ['sum', 'count'];

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select aggregation function'}>
        Aggregation
      </InlineFormLabel>
      <Select
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={aggFunctions.map((name) => ({ label: name, value: name }))}
        value={query.aggregationFunction}
        onChange={(value) => onChange({ ...query, aggregationFunction: value.value })}
      />
    </div>
  );
}

function SelectGroupBy(props: PinotQueryEditorProps) {
  const { datasource, query, onChange } = props;

  const schema = useTableSchema(datasource, query.databaseName, query.tableName);
  const dimensionColumns = schema?.dimensionFieldSpecs.map((spec) => spec.name);

  return (
    <div className={'gf-form'}>
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Select dimensions function'}>
        Group By
      </InlineFormLabel>
      <MultiSelect
        className={`width-15 ${styles.Common.inlineSelect}`}
        options={(dimensionColumns || []).map((name) => ({ label: name, value: name }))}
        value={query.dimensionColumns}
        onChange={(item: SelectableValue<string>[]) => {
          const selected = item.map((v) => v.value).filter((v) => v !== undefined) as string[];
          onChange({ ...query, dimensionColumns: selected });
        }}
      />
    </div>
  );
}

function SqlPreview(props: PinotQueryEditorProps) {
  const { range, query, datasource } = props;
  const sql = useSqlPreview(datasource, {
    aggregationFunction: query.aggregationFunction,
    databaseName: query.databaseName,
    dimensionColumns: query.dimensionColumns,
    intervalSize: 0,
    metricColumn: query.metricColumn,
    tableName: query.tableName,
    timeColumn: query.timeColumn,
    timeRange: { from: range?.to, to: range?.from },
  });

  return (
    <div className="gf-form">
      <InlineFormLabel width={8} className="query-keyword" tooltip={'Sql Preview'}>
        Sql Preview
      </InlineFormLabel>
      <pre>{sql}</pre>
    </div>
  );
}
