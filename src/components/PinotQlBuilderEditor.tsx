import { PinotQueryEditorProps } from '../types/PinotQueryEditorProps';
import { SelectMetricColumn } from './SelectMetricColumn';
import { SelectAggregation } from './SelectAggregation';
import { SelectGroupBy } from './SelectGroupBy';
import { SqlPreview } from './SqlPreview';
import React from 'react';
import { InputLimit } from './InputLimit';
import { SelectFilters } from './SelectFilters';
import { useTableSchema } from '../resources/resources';
import { SelectTimeColumn } from './SelectTimeColumn';
import { canRunQuery, PinotDataQuery } from '../types/PinotDataQuery';

export function PinotQlBuilderEditor(props: PinotQueryEditorProps) {
  const { datasource, query, range, onChange, onRunQuery } = props;

  const tableSchema = useTableSchema(datasource, query.databaseName, query.tableName);

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    if (canRunQuery(newQuery)) {
      onRunQuery();
    }
  };

  return (
    <>
      <div>
        <SelectTimeColumn
          selected={query.timeColumn}
          options={tableSchema?.dateTimeFieldSpecs.map((spec) => spec.name)}
          onChange={(value) => onChangeAndRun({ ...query, timeColumn: value })}
        />
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectMetricColumn
          selected={query.metricColumn}
          options={tableSchema?.metricFieldSpecs.map((spec) => spec.name)}
          onChange={(value) => onChangeAndRun({ ...query, metricColumn: value })}
        />
        <SelectAggregation {...props} />
      </div>
      <div>
        <SelectGroupBy {...props} />
      </div>
      <div>
        <SelectFilters
          datasource={datasource}
          databaseName={query.databaseName}
          tableSchema={tableSchema}
          tableName={query.tableName}
          timeColumn={query.timeColumn}
          range={range}
          dimensionColumns={query.dimensionColumns}
          dimensionFilters={query.dimensionFilters}
          onChange={(val) => onChangeAndRun({ ...props.query, dimensionFilters: val })}
        />
      </div>
      <div>
        <InputLimit {...props} />
      </div>
      <div>
        <SqlPreview {...props} />
      </div>
    </>
  );
}
