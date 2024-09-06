import { SqlEditor } from './SqlEditor';
import React, { useState } from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { PinotDataQuery } from '../../types/PinotDataQuery';
import { fetchSqlCodePreview } from '../../resources/sqlCodePreview';
import { SqlPreview } from './SqlPreview';
import { DisplayTypeTimeSeries, SelectDisplayType } from './SelectDisplayType';
import { SelectTable } from './SelectTable';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';

export function PinotQlCode(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  tables: string[] | undefined;
  onChange: (value: PinotDataQuery) => void;
  onRunQuery: () => void;
}) {
  const { query, tables, timeRange, intervalSize, datasource, onChange, onRunQuery } = props;

  const [sqlPreview, setSqlPreview] = useState('');

  const onChangeAndUpdatePreview = (newQuery: PinotDataQuery) => {
    fetchSqlCodePreview(datasource, {
      intervalSize: intervalSize || '0',
      tableName: newQuery.tableName,
      timeRange: timeRange,
      timeColumnAlias: newQuery.timeColumnAlias,
      timeColumnFormat: newQuery.timeColumnFormat,
      metricColumnAlias: newQuery.metricColumnAlias,
      code: newQuery.pinotQlCode,
    }).then((val) => val && setSqlPreview(val));
    onChange(newQuery);
  };

  if (
    query.displayType === undefined ||
    query.metricColumnAlias === undefined ||
    query.timeColumnAlias === undefined ||
    query.pinotQlCode === undefined
  ) {
    onChangeAndUpdatePreview({
      ...query,
      displayType: query.displayType || DisplayTypeTimeSeries,
      metricColumnAlias: query.metricColumnAlias || 'metric',
      timeColumnAlias: query.timeColumnAlias || 'time',
      pinotQlCode:
        query.pinotQlCode ||
        `
SELECT
  $__timeGroup("${query.timeColumn || 'timestamp'}") AS $__timeAlias(),
  SUM("${query.metricColumn || 'metric'}") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("${query.timeColumn || 'timestamp'}")
GROUP BY $__timeGroup("${query.timeColumn || 'timestamp'}")
ORDER BY $__timeAlias() DESC
LIMIT 100000
`.trim(),
    });
    onRunQuery();
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
        <InputMetricColumnAlias
          current={query.metricColumnAlias}
          onChange={(val) => onChange({ ...query, metricColumnAlias: val })}
        />
      </div>

      <SqlEditor
        current={query.pinotQlCode}
        onChange={(pinotQlCode) => onChangeAndUpdatePreview({ ...query, pinotQlCode })}
      />
      <div>
        <SqlPreview sql={sqlPreview} />
      </div>
      <div>
        <InputMetricLegend
          current={query.legend}
          onChange={(legend) => onChangeAndUpdatePreview({ ...query, legend })}
        />
      </div>
    </div>
  );
}
