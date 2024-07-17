import { PinotQueryEditorProps } from '../../types/PinotQueryEditorProps';
import { SqlEditor } from './SqlEditor';
import React, { useState } from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { InputTimeColumnFormat } from './InputTimeColumnFormat';
import { SqlPreview } from './SqlPreview';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { fetchSqlCodePreview } from '../../resources/sqlCodePreview';

export function PinotQlCode(props: PinotQueryEditorProps) {
  const { query, data, datasource, onChange, onRunQuery } = props;

  const [sqlPreview, setSqlPreview] = useState('');

  const updateSqlPreview = (dataQuery: PinotDataQuery) => {
    fetchSqlCodePreview(datasource, {
      databaseName: dataQuery.databaseName,
      intervalSize: data?.request?.interval || '0',
      tableName: dataQuery.tableName,
      timeRange: { to: props.data?.request?.range.to, from: props.data?.request?.range.from },
      timeColumnAlias: dataQuery.timeColumnAlias,
      timeColumnFormat: dataQuery.timeColumnFormat,
      metricColumnAlias: dataQuery.metricColumnAlias,
      code: dataQuery.pinotQlCode,
    }).then((val) => val && setSqlPreview(val));
  };

  const canRunQuery = (newQuery: PinotDataQuery): boolean => {
    return !!(
      newQuery.databaseName &&
      newQuery.tableName &&
      newQuery.timeColumnAlias &&
      newQuery.timeColumnFormat &&
      newQuery.metricColumnAlias &&
      newQuery.pinotQlCode
    );
  };

  const onChangeAndRun = (newQuery: PinotDataQuery) => {
    onChange(newQuery);
    if (canRunQuery(newQuery)) {
      updateSqlPreview(newQuery);
      onRunQuery();
    }
  };

  if (!sqlPreview) {
    updateSqlPreview(query);
  }

  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <InputTimeColumnAlias
          current={query.timeColumnAlias}
          onChange={(val) => onChangeAndRun({ ...query, timeColumnAlias: val })}
        />
        <InputTimeColumnFormat
          current={query.timeColumnFormat}
          onChange={(val) => onChangeAndRun({ ...query, timeColumnFormat: val })}
        />
      </div>
      <InputMetricColumnAlias
        current={query.metricColumnAlias}
        onChange={(val) => onChange({ ...props.query, metricColumnAlias: val })}
      />
      <SqlEditor current={query.pinotQlCode} onChange={(val) => onChange({ ...props.query, pinotQlCode: val })} />
      {/*<SqlPreview sql={sqlPreview} />*/}
    </div>
  );
}
