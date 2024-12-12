import { SqlEditor } from './SqlEditor';
import React from 'react';
import { InputTimeColumnAlias } from './InputTimeColumnAlias';
import { InputMetricColumnAlias } from './InputMetricColumnAlias';
import { PinotDataQuery } from '../../dataquery/PinotDataQuery';
import { SqlPreview } from './SqlPreview';
import { DisplayTypeLogs, DisplayTypeTable, DisplayTypeTimeSeries, SelectDisplayType } from './SelectDisplayType';
import { DateTime } from '@grafana/data';
import { DataSource } from '../../datasource';
import { InputMetricLegend } from './InputMetricLegend';
import { InputLogColumnAlias } from './InputLogColumnAlias';
import { SelectTable } from './SelectTable';
import { canRunCodeQuery, CodeParams } from '../../pinotql/codeParams';
import { useCodeResources } from '../../pinotql/codeResources';

export function PinotQlCode(props: {
  query: PinotDataQuery;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  intervalSize: string | undefined;
  datasource: DataSource;
  savedParams: CodeParams;
  interpolatedParams: CodeParams;
  onChange: (newParams: CodeParams) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, intervalSize, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;

  const resources = useCodeResources(datasource, timeRange, intervalSize, interpolatedParams);
  const onChangeAndRun = (newParams: CodeParams) => {
    onChange(newParams);
    if (canRunCodeQuery(newParams)) {
      onRunQuery();
    }
  };

  return (
    <div>
      <SelectDisplayType
        value={savedParams.displayType}
        onChange={(displayType) => onChangeAndRun({ ...savedParams, displayType })}
      />
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        <div className={'gf-form'} data-testid="select-table">
          <SelectTable
            options={resources.tables}
            selected={savedParams.tableName}
            isLoading={resources.isTablesLoading}
            onChange={(tableName) => onChange({ ...savedParams, tableName })}
          />
        </div>
      </div>
      {savedParams.displayType === DisplayTypeTable && (
        <div style={{ display: 'flex', flexDirection: 'row' }}>
          <InputTimeColumnAlias
            current={savedParams.timeColumnAlias}
            onChange={(timeColumnAlias) => onChange({ ...savedParams, timeColumnAlias })}
          />
        </div>
      )}
      {savedParams.displayType === DisplayTypeTimeSeries && (
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
      {savedParams.displayType === DisplayTypeLogs && (
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
        <InputMetricLegend current={savedParams.legend} onChange={(legend) => onChange({ ...savedParams, legend })} />
      </div>
    </div>
  );
}
