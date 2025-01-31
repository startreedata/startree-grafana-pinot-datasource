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
    <div>
      {savedParams.displayType !== DisplayType.ANNOTATIONS && (
        <SelectDisplayType
          value={savedParams.displayType}
          onChange={(displayType) => onChangeAndRun({ ...savedParams, displayType })}
        />
      )}

      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <SelectTable
          options={resources.tables}
          selected={savedParams.tableName}
          isLoading={resources.isTablesLoading}
          onChange={(tableName) => onChangeAndRun({ ...savedParams, tableName })}
        />
      </div>
      {savedParams.displayType === DisplayType.TABLE && (
        <InputTimeColumnAlias
          current={savedParams.timeColumnAlias}
          onChange={(timeColumnAlias) => onChange({ ...savedParams, timeColumnAlias })}
        />
      )}
      {savedParams.displayType === DisplayType.TIMESERIES && (
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
      />

      <div>
        <SqlPreview sql={resources.sqlPreview} />
      </div>
      <div>
        <InputMetricLegend
          current={savedParams.legend}
          displayType={savedParams.displayType}
          onChange={(legend) => onChange({ ...savedParams, legend })}
        />
      </div>
    </div>
  );
}
