import { SqlEditor } from './SqlEditor';
import React, { useEffect, useState } from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { interpolateVariables, PinotDataQuery } from '../../types/PinotDataQuery';
import { SqlPreview } from './SqlPreview';
import { DisplayTypeLogs, DisplayTypeTable, DisplayTypeTimeSeries, SelectDisplayType } from './SelectDisplayType';
import { DateTime, ScopedVars } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { previewSqlCode, PreviewSqlCodeRequest } from '../../resources/previewSql';
import { InputLogColumnAlias } from './InputLogColumnAlias';
import {SelectTable} from "./SelectTable";

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

  if (!query.displayType) {
    onChange({
      ...query,
      displayType: query.displayType || DisplayTypeTimeSeries,
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
            onChange={(tableName) => onChange({ ...query, tableName, filters: undefined })}
          />
        </div>
      </div>
      {query.displayType === DisplayTypeTable && (
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <InputTimeColumnAlias
            current={query.timeColumnAlias}
            onChange={(val) => onChange({ ...query, timeColumnAlias: val })}
          />
        </div>
      )}
      {query.displayType === DisplayTypeTimeSeries && (
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
      )}
      {query.displayType === DisplayTypeLogs && (
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <InputTimeColumnAlias
            current={query.timeColumnAlias}
            onChange={(val) => onChange({ ...query, timeColumnAlias: val })}
          />
          <InputLogColumnAlias
            current={query.logColumnAlias}
            onChange={(val) => onChange({ ...query, logColumnAlias: val })}
          />
        </div>
      )}
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
    timeRange: {
      to: timeRange.to?.endOf('second'),
      from: timeRange.from?.startOf('second'),
    },
    tableName: interpolated.tableName,
    timeColumnAlias: interpolated.timeColumnAlias,
    timeColumnFormat: interpolated.timeColumnFormat,
    metricColumnAlias: interpolated.metricColumnAlias,
    code: interpolated.pinotQlCode,
  };

  useEffect(() => {
    previewSqlCode(datasource, previewRequest).then((val) => val && setSqlPreview(val));
  }, [datasource, query.queryType, query.editorMode, JSON.stringify(previewRequest)]); // eslint-disable-line react-hooks/exhaustive-deps

  return sqlPreview;
}
