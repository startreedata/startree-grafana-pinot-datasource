import { DateTime } from '@grafana/data';
import { Button } from '@grafana/ui';
import { DataSource } from '../../datasource';
import { SelectTable } from './SelectTable';
import { SelectTimeColumn } from './SelectTimeColumn';
import { SelectFilters } from './SelectFilters';
import { SelectQueryOptions } from './SelectQueryOptions';
import { InputLimit } from './InputLimit';
import { SqlPreview } from './SqlPreview';
import React, { useEffect } from 'react';
import { SelectLogMessageColumn } from './SelectLogMessageColumn';
import { SelectLevelColumn } from './SelectLevelColumn';
import { SelectJsonExtractors } from './SelectJsonExtractors';
import { SelectMetadataColumns } from './SelectMetadataColumns';
import { SelectRegexpExtractors } from './SelectRegexpExtractors';
import { LogsBuilder } from '../../pinotql';
import allLabels from '../../labels';
import { useAutoSurfaceMultiStageEngine } from './useAutoSurfaceMultiStageEngine';

export function PinotQlLogsBuilder(props: {
  savedParams: LogsBuilder.Params;
  interpolatedParams: LogsBuilder.Params;
  timeRange: { to: DateTime | undefined; from: DateTime | undefined };
  datasource: DataSource;
  onChange: (value: LogsBuilder.Params) => void;
  onRunQuery: () => void;
}) {
  const { timeRange, datasource, savedParams, interpolatedParams, onChange, onRunQuery } = props;
  const resources = LogsBuilder.useResources(datasource, timeRange, interpolatedParams);
  const otelLabels = allLabels.components.QueryEditor.otelDefaults;

  const onChangeAndRun = (newParams: LogsBuilder.Params) => {
    onChange(newParams);
    if (LogsBuilder.canRunQuery(newParams)) {
      onRunQuery();
    }
  };

  useEffect(() => {
    if (LogsBuilder.applyDefaults(savedParams, resources)) {
      onChangeAndRun({ ...savedParams });
    }
  });

  useAutoSurfaceMultiStageEngine(savedParams, interpolatedParams, onChangeAndRun);

  return (
    <>
      <SelectTable
        options={resources.tables}
        selected={savedParams.tableName}
        isLoading={resources.isTablesLoading}
        onChange={(tableName) => onChange({ ...savedParams, tableName })}
      />
      <SelectTimeColumn
        selected={savedParams.timeColumn}
        timeColumns={resources.timeColumns}
        isLoading={resources.isColumnsLoading}
        onChange={(value) => onChangeAndRun({ ...savedParams, timeColumn: value })}
      />
      <div className={'gf-form'}>
        <Button
          data-testid="apply-otel-defaults-btn"
          size="sm"
          variant="secondary"
          icon="cog"
          tooltip={otelLabels.tooltip}
          onClick={() => onChangeAndRun(LogsBuilder.otelDefaults(savedParams))}
        >
          {otelLabels.label}
        </Button>
      </div>
      <div style={{ display: 'flex', flexDirection: 'row' }}>
        <SelectLogMessageColumn
          selected={savedParams.logColumn}
          columns={resources.logMessageColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(logColumn) => onChangeAndRun({ ...savedParams, logColumn })}
        />
        <SelectLevelColumn
          selected={savedParams.levelColumn}
          columns={resources.filterColumns}
          isLoading={resources.isColumnsLoading}
          onChange={(levelColumn) => onChangeAndRun({ ...savedParams, levelColumn })}
        />
        <SelectMetadataColumns
          selected={savedParams.metadataColumns}
          columns={resources.columns.filter(
            ({ name, key }) =>
              savedParams.timeColumn !== name &&
              (savedParams.logColumn?.name !== name || savedParams.logColumn?.key !== key)
          )}
          isLoading={resources.isColumnsLoading}
          onChange={(metadataColumns) => onChangeAndRun({ ...savedParams, metadataColumns })}
        />
      </div>
      <SelectJsonExtractors
        extractors={savedParams.jsonExtractors}
        columns={resources.jsonExtractorColumns}
        isLoadingColumns={resources.isColumnsLoading}
        onChange={(jsonExtractors) => onChangeAndRun({ ...savedParams, jsonExtractors })}
      />
      <SelectRegexpExtractors
        extractors={savedParams.regexpExtractors}
        columns={resources.regexpExtractorColumns}
        isLoadingColumns={resources.isColumnsLoading}
        onChange={(regexpExtractors) => onChangeAndRun({ ...savedParams, regexpExtractors })}
      />
      <SelectFilters
        datasource={datasource}
        tableName={savedParams.tableName}
        timeColumn={savedParams.timeColumn}
        timeRange={timeRange}
        columns={resources.filterColumns}
        filters={savedParams.filters}
        isColumnsLoading={resources.isColumnsLoading}
        onChange={(val) => onChangeAndRun({ ...savedParams, filters: val })}
      />
      <SelectQueryOptions
        selected={savedParams.queryOptions}
        onChange={(queryOptions) => onChangeAndRun({ ...savedParams, queryOptions })}
      />
      <InputLimit current={savedParams.limit} onChange={(limit) => onChangeAndRun({ ...savedParams, limit })} />
      <SqlPreview sql={resources.sqlPreview} />
    </>
  );
}
