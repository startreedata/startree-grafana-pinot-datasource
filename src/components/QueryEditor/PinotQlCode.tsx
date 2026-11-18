import { SqlEditor } from './SqlEditor';
import React from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { SqlPreview } from './SqlPreview';
import { SelectDisplayType } from './SelectDisplayType';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { InputLogColumnAlias } from './InputLogColumnAlias';
import { SelectTable } from './SelectTable';
import { DisplayType } from '../../dataquery/DisplayType';
import { CodeQuery } from '../../pinotql';
import { InputSeriesLimit } from './InputLimit';
import { SelectQueryOptions } from './SelectQueryOptions';
import { Alert } from '@grafana/ui';
import { sqlUsesTimeRange } from '../../pinotql/timeRange';

export function PinotQlCode(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  savedParams: CodeQuery.Params;
  interpolatedParams: CodeQuery.Params;
  onChange: (newParams: CodeQuery.Params) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, intervalSize, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;

  const resources = CodeQuery.useResources(datasource, timeRange, intervalSize, interpolatedParams);
  CodeQuery.applyDefaults(savedParams);

  const onChangeAndRun = (newParams: CodeQuery.Params) => {
    onChange(newParams);
    onRunQuery();
  };

  return (
    <>
      {savedParams.displayType !== DisplayType.ANNOTATIONS && (
        <SelectDisplayType
          value={savedParams.displayType}
          onChange={(displayType) => onChangeAndRun({ ...savedParams, displayType })}
        />
      )}

      <SelectTable
        options={resources.tables}
        selected={savedParams.tableName}
        isLoading={resources.isTablesLoading}
        onChange={(tableName) => onChangeAndRun({ ...savedParams, tableName })}
      />
      {savedParams.displayType === DisplayType.TABLE && (
        <InputTimeColumnAlias
          current={savedParams.timeColumnAlias}
          onChange={(timeColumnAlias) => onChange({ ...savedParams, timeColumnAlias })}
        />
      )}
      {savedParams.displayType === DisplayType.TIMESERIES && (
        <>
          <div style={{ display: 'flex', flexDirection: 'row' }}>
            <InputTimeColumnAlias
              current={savedParams.timeColumnAlias}
              onChange={(timeColumnAlias) => onChange({ ...savedParams, timeColumnAlias })}
            />
            <InputMetricColumnAlias
              current={savedParams.metricColumnAlias}
              onChange={(metricColumnAlias) => onChange({ ...savedParams, metricColumnAlias })}
            />
          </div>
        </>
      )}
      {savedParams.displayType === DisplayType.LOGS && (
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <InputTimeColumnAlias
            current={savedParams.timeColumnAlias}
            onChange={(timeColumnAlias) => onChange({ ...savedParams, timeColumnAlias })}
          />
          <InputLogColumnAlias
            current={savedParams.logColumnAlias}
            onChange={(logColumnAlias) => onChange({ ...savedParams, logColumnAlias })}
          />
        </div>
      )}
      <SqlEditor
        current={savedParams.pinotQlCode}
        onChange={(pinotQlCode) => onChange({ ...savedParams, pinotQlCode })}
        datasource={datasource}
        tableName={savedParams.tableName}
      />
      {savedParams.displayType !== DisplayType.ANNOTATIONS &&
        savedParams.pinotQlCode?.trim() &&
        !sqlUsesTimeRange(savedParams.pinotQlCode) && (
          <Alert
            title="This query ignores the dashboard time range"
            severity="warning"
            data-testid="no-time-range-warning"
          >
            Changing the time picker will not change the results. Add a time filter such as{' '}
            <code>WHERE $__timeFilter</code> (uses the table&apos;s time column) or{' '}
            <code>WHERE $__timeFilter(&quot;&lt;time column&gt;&quot;)</code>. If the query ignores the time range on
            purpose, add the comment <code>-- $__noTimeRange</code> to hide this warning.
          </Alert>
        )}
      <SelectQueryOptions
        selected={savedParams.queryOptions}
        onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
      />
      <SqlPreview sql={resources.sqlPreview} />
      {savedParams.displayType === DisplayType.TIMESERIES && (
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <InputMetricLegend
            current={savedParams.legend}
            onChange={(legend) => onChangeAndRun({ ...savedParams, legend })}
          />
          <InputSeriesLimit
            current={savedParams.seriesLimit}
            onChange={(seriesLimit) => onChange({ ...savedParams, seriesLimit })}
          />
        </div>
      )}
    </>
  );
}
