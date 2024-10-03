import { SqlEditor } from './SqlEditor';
import React, { useEffect, useState } from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { interpolateVariables, PinotDataQuery } from '../../types/PinotDataQuery';
import { SqlPreview } from './SqlPreview';
import { DisplayTypeTimeSeries, SelectDisplayType } from './SelectDisplayType';
import { SelectTable } from './SelectTable';
import { DateTime, ScopedVars } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { previewSqlCode, PreviewSqlCodeRequest } from '../../resources/previewSql';

export function PinotQlCode(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  tables: string[] | undefined;
  onChange: (value: PinotDataQuery) => void;
  onRunQuery: () => void;
  scopedVars: ScopedVars;
}) {
  const { query, tables, timeRange, intervalSize, datasource, scopedVars, onChange, onRunQuery } = props;

  const sqlPreview = useSqlPreview(datasource, intervalSize, timeRange, query, scopedVars);

  const defaultSql = (query: PinotDataQuery) => `SELECT $__timeGroup("${query.timeColumn || 'timestamp'}") AS $__timeAlias(), SUM("${query.metricColumn || 'metric'}") AS $__metricAlias()
FROM $__table()
WHERE $__timeFilter("${query.timeColumn || 'timestamp'}")
GROUP BY $__timeGroup("${query.timeColumn || 'timestamp'}")
ORDER BY $__timeAlias() DESC
LIMIT 100000`;

  if (!query.displayType || !query.pinotQlCode) {
    onChange({
      ...query,
      displayType: query.displayType || DisplayTypeTimeSeries,
      pinotQlCode: query.pinotQlCode || defaultSql(query),
    });
    onRunQuery();
  }

  return (
    <div>
      <SelectDisplayType
        value={query.displayType}
        onChange={(val) => {
          onChange({ ...query, displayType: val });
          onRunQuery();
        }}
      />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'} data-testid="select-table">
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
          onChange={(val) => onChange({ ...query, timeColumnAlias: val })}
        />
        <InputMetricColumnAlias
          current={query.metricColumnAlias}
          onChange={(val) => onChange({ ...query, metricColumnAlias: val })}
        />
      </div>

      <SqlEditor current={query.pinotQlCode} onChange={(pinotQlCode) => onChange({ ...query, pinotQlCode })} />

      <div>
        <SqlPreview sql={sqlPreview} />
      </div>
      <div>
        <InputMetricLegend current={query.legend} onChange={(legend) => onChange({ ...query, legend })} />
      </div>
    </div>
  );
}

function useSqlPreview(
  datasource: DataSource,
  intervalSize: string | undefined,
  timeRange: {
    to: DateTime | undefined;
    from: DateTime | undefined;
  },
  query: PinotDataQuery,
  scopedVars: ScopedVars
): string {
  const [sqlPreview, setSqlPreview] = useState('');

  const interpolated = interpolateVariables(query, scopedVars);
  const previewRequest: PreviewSqlCodeRequest = {
    intervalSize: intervalSize,
    timeRange: timeRange,
    tableName: interpolated.tableName,
    timeColumnAlias: interpolated.timeColumnAlias,
    timeColumnFormat: interpolated.timeColumnFormat,
    metricColumnAlias: interpolated.metricColumnAlias,
    code: interpolated.pinotQlCode,
  };

  useEffect(() => {
    previewSqlCode(datasource, previewRequest).then((val) => val && setSqlPreview(val));
  }, [datasource, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps

  return sqlPreview;
}
