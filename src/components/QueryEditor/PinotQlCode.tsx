import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { SqlEditor } from './SqlEditor';
import React, { useState } from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { InputTimeColumnFormat } from './InputTimeColumnFormat';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { fetchSqlCodePreview } from '../../resources/sqlCodePreview';
import { SqlPreview } from './SqlPreview';
import { SelectDisplayType } from './SelectDisplayType';
import { SelectTable } from './SelectTable';
import { useTables } from '../../resources/controller';

export function PinotQlCode(props: PinotQueryEditorProps) {
  const { query, data, datasource, onChange, onRunQuery } = props;

  const [sqlPreview, setSqlPreview] = useState('');

  const tables = useTables(datasource);

  const updateSqlPreview = (dataQuery: PinotDataQuery) => {
    fetchSqlCodePreview(datasource, {
      intervalSize: data?.request?.interval || '0',
      tableName: dataQuery.tableName,
      timeRange: { to: props.data?.request?.range.to, from: props.data?.request?.range.from },
      timeColumnAlias: dataQuery.timeColumnAlias,
      timeColumnFormat: dataQuery.timeColumnFormat,
      metricColumnAlias: dataQuery.metricColumnAlias,
      code: dataQuery.pinotQlCode,
    }).then((val) => val && setSqlPreview(val));
  };

  const onChangeAndUpdatePreview = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    updateSqlPreview(newQuery);
  };

  if (!sqlPreview) {
    updateSqlPreview(query);
  }

  return (
    <div>
      <SelectDisplayType
        value={query.displayType}
        onChange={(val) => {
          onChangeAndUpdatePreview({ ...query, displayType: val });
          onRunQuery();
        }}
      />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'}>
          <SelectTable
            options={tables}
            selected={query.tableName}
            onChange={(value: string | undefined) =>
              onChange({
                ...query,
                tableName: value,
                timeColumn: undefined,
                metricColumn: undefined,
                groupByColumns: undefined,
                aggregationFunction: undefined,
                filters: undefined,
              })
            }
          />
        </div>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputTimeColumnAlias
          current={query.timeColumnAlias}
          onChange={(val) => onChangeAndUpdatePreview({ ...query, timeColumnAlias: val })}
        />
        <InputTimeColumnFormat
          current={query.timeColumnFormat}
          onChange={(val) => onChangeAndUpdatePreview({ ...query, timeColumnFormat: val })}
        />
      </div>
      <InputMetricColumnAlias
        current={query.metricColumnAlias}
        onChange={(val) => onChange({ ...query, metricColumnAlias: val })}
      />
      <SqlEditor
        current={query.pinotQlCode}
        onChange={(val) => onChangeAndUpdatePreview({ ...props.query, pinotQlCode: val })}
      />
      <SqlPreview sql={sqlPreview} />
    </div>
  );
}
